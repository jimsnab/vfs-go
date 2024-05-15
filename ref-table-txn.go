package vfs

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/jimsnab/afero"
)

type (
	refTableTransaction struct {
		mu                sync.Mutex
		table             *refTable
		txn               *avlTransaction
		testRemovedShards map[uint64]struct{}
	}
)

var ErrShardRemoved = errors.New("shard removed")

func (table *refTable) BeginTransaction(tm *transactionManager) (txn *refTableTransaction, err error) {
	table.mu.Lock()
	defer table.mu.Unlock()
	return table.doBeginTransaction(tm)
}

func (table *refTable) doBeginTransaction(tm *transactionManager) (txn *refTableTransaction, err error) {
	if table.cancelFn == nil {
		err = ErrNotStarted
		return
	}

	if table.txn != nil {
		err = ErrTransactionStarted
		return
	}

	treeTxn, err := table.index.BeginTransaction(tm)
	if err != nil {
		return
	}

	txn = &refTableTransaction{
		table: table,
		txn:   treeTxn,
	}
	return
}

func (txn *refTableTransaction) EndTransaction() (err error) {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.doEndTransaction()
}

func (txn *refTableTransaction) doEndTransaction() (err error) {
	err = txn.txn.EndTransaction()
	txn.table.txn = nil
	return
}

func (txn *refTableTransaction) AddReferences(refRecords []refRecord) (err error) {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	txn.table.mu.Lock()
	defer txn.table.mu.Unlock()

	return txn.doAddReferences(refRecords, 0)
}

func (txn *refTableTransaction) AddReferencesAtShard(refRecords []refRecord, refShard uint64) (err error) {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	txn.table.mu.Lock()
	defer txn.table.mu.Unlock()

	return txn.doAddReferences(refRecords, refShard)
}

func (txn *refTableTransaction) doAddReferences(refRecords []refRecord, refShard uint64) (err error) {
	table := txn.table
	if table.cancelFn == nil {
		err = ErrNotStarted
		return
	}

	f, shard, err := table.openShard(refShard, false)
	if err != nil {
		return
	}

	newArrays := make([][][20]byte, 0, len(refRecords))
	valueKeyPos := make(map[[20]byte]int, len(refRecords))
	valueKeyFromPos := make(map[int][20]byte, len(refRecords))
	keyGroups := make(map[[20]byte]string, len(refRecords))

	// combine records with previously stored arrays
	for _, record := range refRecords {
		// make a fixed array for the value key
		valueKey := record.ValueKey

		// append to prior refs array when two or more refs on the same value key;
		// load the refs array for the first occurance of the value key
		var refs [][20]byte
		vkp, exists := valueKeyPos[valueKey]
		if !exists {
			if refs, err = txn.doRetrieveReferences(record.KeyGroup, valueKey); err != nil {
				return
			}
			vkp = len(newArrays)
			valueKeyPos[valueKey] = vkp
			valueKeyFromPos[vkp] = valueKey
			keyGroups[valueKey] = record.KeyGroup
			newArrays = append(newArrays, refs)
		} else {
			if keyGroups[valueKey] != record.KeyGroup {
				err = errors.New("a value key must belong to a single key group")
				return
			}
			refs = newArrays[vkp]
		}

		// if reached reference limit, throw out the request
		if table.cfg.ReferenceLimit > 0 && len(refs) >= int(table.cfg.ReferenceLimit) {
			continue
		}

		// if reference is already in the list, don't add it again
		found := false
		for _, ref := range refs {
			if keysEqual(ref, record.StoreKey) {
				found = true
				break
			}
		}
		if found {
			continue
		}

		// add to what needs to be written
		newArrays[vkp] = append(newArrays[vkp], record.StoreKey)
	}

	// write new arrays
	for i, refs := range newArrays {
		valueKey := valueKeyFromPos[i]

		var pos int
		var flat []byte
		if table.storeKeyInData {
			end := 24 + (len(refs) * 20)
			flat = make([]byte, end)
			binary.BigEndian.PutUint32(flat[0:4], uint32(end-24))
			copy(flat[4:24], valueKey[:])
			pos = 24
		} else {
			end := 4 + (len(refs) * 20)
			flat = make([]byte, end)
			binary.BigEndian.PutUint32(flat[0:4], uint32(end-4))
			pos = 4
		}

		for _, ref := range refs {
			copy(flat[pos:pos+20], ref[:])
			pos += 20
		}

		var offset int64
		if offset, err = f.Seek(0, io.SeekEnd); err != nil {
			return
		}

		var n int
		n, err = f.Write(flat)
		if err != nil {
			return
		}

		if n < len(flat) {
			err = errors.New("unable to write all data") // should be unreachable
			return
		}

		if _, err = txn.txn.Set(keyGroups[valueKey], valueKey, shard, uint64(offset)); err != nil {
			return
		}
	}

	if table.cfg.Sync {
		if err = f.Sync(); err != nil {
			return
		}
	}

	return
}

func (txn *refTableTransaction) RetrieveReferences(keyGroup string, valueKey [20]byte) (refs [][20]byte, err error) {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	txn.table.mu.Lock()
	defer txn.table.mu.Unlock()

	return txn.doRetrieveReferences(keyGroup, valueKey)
}

func (txn *refTableTransaction) doRetrieveReferences(keyGroup string, valueKey [20]byte) (refs [][20]byte, err error) {
	if txn.table.cancelFn == nil {
		err = ErrNotStarted
		return
	}

	found, shard, position, _, err := txn.txn.Get(keyGroup, valueKey)
	if err != nil {
		return
	}

	if !found {
		return
	}

	var f afero.File
	f, _, err = txn.table.openShard(shard, true)
	if err != nil {
		if strings.HasSuffix(err.Error(), os.ErrNotExist.Error()) {
			// valueKey indexed but shard is gone; treat as if it doesn't exist
			err = nil

			// testing only
			if txn.testRemovedShards != nil {
				_, removed := txn.testRemovedShards[shard]
				if removed {
					err = ErrShardRemoved
				}
			}
		}
		return
	}

	hdr := make([]byte, 4)
	var n int
	if n, err = f.ReadAt(hdr, int64(position)); err != nil {
		return
	}

	if n != 4 {
		err = errors.New("short read for reference array length")
		return
	}

	recordSize := int(int32(binary.BigEndian.Uint32(hdr)))
	if recordSize < 0 {
		err = errors.New("reference array was deleted")
		return
	}
	if recordSize%20 != 0 {
		err = errors.New("reference array size not integral")
		return
	}
	items := recordSize / 20

	arrayPos := int64(position)
	if txn.table.storeKeyInData {
		// skip over value key that was copied into the data file
		arrayPos += 24
	} else {
		arrayPos += 4
	}

	data := make([]byte, recordSize)
	if n, err = f.ReadAt(data, arrayPos); err != nil {
		return
	}

	if n != recordSize {
		err = errors.New("short read for reference array")
		return
	}

	refs = make([][20]byte, 0, items)
	for i := 0; i < recordSize; i += 20 {
		ref := [20]byte{}
		copy(ref[:], data[i:i+20])
		refs = append(refs, ref)
	}

	return
}
