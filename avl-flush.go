package vfs

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
)

type (
	CommitCompleted func(err error)
)

// inner function - does not call onComplete when it returns non nil err
func (tree *avlTree) flush(onComplete CommitCompleted) (err error) {
	// back up everything
	hasNew := (tree.allocatedSize != tree.committedSize)
	hasBackup := false

	if tree.cfg.RecoveryEnabled {
		hasBackup = hasNew

		// ensure the last recovery file change completed
		tree.dt2sync.Wait()

		var priorSize int64
		if priorSize, err = tree.rf.Seek(0, io.SeekEnd); err != nil {
			return
		}
		if priorSize != 0 {
			err = errors.New("can't overwrite pending recovery records")
			return
		}

		w := bufio.NewWriter(tree.rf)

		for _, node := range tree.writtenNodes {
			if node.originalRawNode != nil {
				if err = tree.backUp(w, node.offset, node.originalRawNode); err != nil {
					return
				}
				hasBackup = true
			}
		}

		for _, node := range tree.freeNodes {
			if node != nil && node.dirty && node.originalRawFree != nil {
				if err = tree.backUp(w, node.offset, node.originalRawFree); err != nil {
					return
				}
				hasBackup = true
			}
		}

		// always back up the header
		if hasBackup {
			if err = tree.backUp(w, 0, tree.originalRawHdr); err != nil {
				return
			}

			// backup complete - ensure it gets to disk
			if err = w.Flush(); err != nil {
				return
			}

			if err = tree.rf.Sync(); err != nil {
				return
			}
		}
	}

	// ensure last sync completed
	tree.dt1sync.Wait()

	// update the commit file size
	if hasNew {
		tree.committedSize = tree.allocatedSize
		tree.dirty = true
		if err = tree.f.Truncate(int64(tree.committedSize)); err != nil {
			return
		}
	}

	// write all the changes
	for _, node := range tree.writtenNodes {
		if err = node.write(); err != nil {
			return
		}
	}

	for _, node := range tree.freeNodes {
		if node != nil && node.dirty {
			if err = node.write(); err != nil {
				return
			}
		}
	}

	if tree.dirty {
		if err = tree.write(); err != nil {
			return
		}
	}

	if tree.cfg.SyncTask {
		tree.dt1sync.Add(1)
		go func() {
			defer tree.dt1sync.Done()
			if err = tree.f.Sync(); err != nil {
				return
			}
		}()
	} else if tree.cfg.Sync {
		if err = tree.f.Sync(); err != nil {
			return
		}
	}

	// success - discard recovery data
	if hasBackup {
		tree.dt2sync.Add(1)
		go func() {
			err := func() error {
				defer tree.dt2sync.Done()

				tree.dt1sync.Wait()

				err := tree.rf.Truncate(0)
				if err != nil {
					return err
				}

				return tree.rf.Sync()
			}()
			if onComplete != nil {
				onComplete(err)
			}
		}()
	} else {
		if onComplete != nil {
			onComplete(nil)
		}
	}

	// purge write queue
	tree.writtenNodes = tree.writtenNodes[:0]

	// toss old allocs
	tree.allocLru.Collect()
	return
}

func (af *avlTree) backUp(w *bufio.Writer, offset uint64, content []byte) (err error) {
	o := [8]byte{}
	binary.BigEndian.PutUint64(o[:], offset)
	n, err := w.Write(o[:])
	if err != nil {
		return
	}
	if n != 8 {
		panic("short write")
	}

	n, err = w.Write(content)
	if err != nil {
		return
	}
	if n != len(content) {
		panic("short write")
	}

	return
}
