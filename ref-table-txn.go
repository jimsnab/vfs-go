package vfs

import (
	"bytes"
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
		mu    sync.Mutex
		table *refTable
		txn   *avlTransaction
	}
)

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
	return txn.txn.EndTransaction()
}

func (txn *refTableTransaction) AddReferences(refRecords []refRecord) (err error) {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	return txn.doAddReferences(refRecords)
}

func (txn *refTableTransaction) doAddReferences(refRecords []refRecord) (err error) {
	table := txn.table
	if table.cancelFn == nil {
		err = ErrNotStarted
		return
	}

	f, shard, err := table.openShard(0, false)
	if err != nil {
		return
	}

	newArrays := make([][][]byte, 0, len(refRecords))
	valueKeyPos := make(map[[20]byte]int, len(refRecords))
	valueKeyFromPos := make(map[int][20]byte, len(refRecords))
	keyGroups := make(map[[20]byte]string, len(refRecords))

	// combine records with previously stored arrays
	for _, record := range refRecords {
		// make a fixed array for the value key
		valueKey := [20]byte{}
		copy(valueKey[:], record.ValueKey)

		// append to prior refs array when two or more refs on the same value key;
		// load the refs array for the first occurance of the value key
		var refs [][]byte
		vkp, exists := valueKeyPos[valueKey]
		if !exists {
			if refs, err = txn.doRetrieveReferences(record.KeyGroup, valueKey[:]); err != nil {
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

		// if reference is already in the list, don't add it again
		found := false
		for _, ref := range refs {
			if bytes.Equal(ref, record.StoreKey) {
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

		end := 4 + (len(refs) * 20)
		flat := make([]byte, end)
		binary.BigEndian.PutUint32(flat[0:4], uint32(end-4))

		pos := 4
		for _, ref := range refs {
			copy(flat[pos:pos+20], ref)
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

		if txn.txn.Set(keyGroups[valueKey], valueKey[:], shard, uint64(offset)); err != nil {
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

func (txn *refTableTransaction) RetrieveReferences(keyGroup string, valueKey []byte) (refs [][]byte, err error) {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.doRetrieveReferences(keyGroup, valueKey)
}

func (txn *refTableTransaction) doRetrieveReferences(keyGroup string, valueKey []byte) (refs [][]byte, err error) {
	if txn.table.cancelFn == nil {
		err = ErrNotStarted
		return
	}

	found, shard, position, err := txn.txn.Get(keyGroup, valueKey)
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
	if recordSize > 20*(1<<14) {
		err = errors.New("reference array size too long")
		return
	}
	if recordSize < 0 {
		err = errors.New("reference array was deleted")
		return
	}
	if recordSize%20 != 0 {
		err = errors.New("reference array size not integral")
		return
	}
	items := recordSize / 20

	data := make([]byte, recordSize)
	if n, err = f.ReadAt(data, int64(position+4)); err != nil {
		return
	}

	if n != recordSize {
		err = errors.New("short read for reference array")
		return
	}

	refs = make([][]byte, 0, items)
	for i := 0; i < recordSize; i += 20 {
		ref := data[i : i+20]
		refs = append(refs, ref)
	}

	return
}