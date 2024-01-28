package vfs

import (
	"io"
)

func (af *avlTreeS) flush() (err error) {
	err = af.lastError()
	if err != nil {
		return
	}

	// update the commit file size
	size, err := af.f.Seek(0, io.SeekEnd)
	if err != nil {
		af.err.Store(&err)
		return
	}

	if size != int64(af.committedSize) {
		af.committedSize = uint64(size)
		af.dirty = true
	}

	// back up everything
	if af.cfg.RecoveryEnabled {

		// ensure the last recovery file change completed
		af.dt2sync.Wait()

		if _, err = af.rf.Seek(0, io.SeekEnd); err != nil {
			af.err.Store(&err)
			return
		}

		for _, node := range af.writtenNodes {
			if node.originalRaw != nil {
				if _, err = af.rf.Write(node.originalRaw); err != nil {
					af.err.Store(&err)
					return
				}
			}
		}

		for _, node := range af.freeNodes {
			if node.dirty && node.originalRaw != nil {
				if _, err = af.rf.Write(node.originalRaw); err != nil {
					af.err.Store(&err)
					return
				}
			}
		}

		if af.dirty {
			if _, err = af.rf.Write(af.originalRaw); err != nil {
				af.err.Store(&err)
				return
			}
		}

		if err = af.rf.Sync(); err != nil {
			af.err.Store(&err)
			return
		}
	}

	// let the previous write get fully committed before writing more
	af.dt1sync.Wait()

	// write all the changes
	for _, node := range af.writtenNodes {
		if err = node.write(); err != nil {
			af.err.Store(&err)
			return
		}
	}

	for _, node := range af.freeNodes {
		if node.dirty {
			if err = node.write(); err != nil {
				af.err.Store(&err)
				return
			}
		}
	}

	if af.dirty {
		if err = af.write(); err != nil {
			af.err.Store(&err)
			return
		}
	}

	if af.cfg.SyncTask {
		af.dt1sync.Add(1)
		go func() {
			defer af.dt1sync.Done()
			if err = af.f.Sync(); err != nil {
				af.err.Store(&err)
				return
			}
		}()
	} else if af.cfg.Sync {
		if err = af.f.Sync(); err != nil {
			af.err.Store(&err)
			return
		}
	}

	// success - discard recovery data
	if af.cfg.RecoveryEnabled {
		if err = af.rf.Truncate(0); err != nil {
			af.err.Store(&err)
			return
		}

		af.dt2sync.Add(1)
		go func() {
			defer af.dt2sync.Done()

			if err = af.rf.Sync(); err != nil {
				af.err.Store(&err)
				return
			}
		}()
	}

	// remove the dirty flags and purge old loaded data
	af.dirty = false
	for _, node := range af.writtenNodes {
		node.dirty = false
	}
	af.writtenNodes = af.writtenNodes[:0]

	for _, node := range af.freeNodes {
		node.dirty = false
	}

	af.allocLru.Collect()
	af.freeLru.Collect()
	return
}
