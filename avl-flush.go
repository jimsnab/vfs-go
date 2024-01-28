package vfs

import (
	"io"
)

func (af *avlTreeS) flush() (err error) {
	if af.err != nil {
		err = af.err
		return
	}

	// update the commit file size
	size, err := af.f.Seek(0, io.SeekEnd)
	if err != nil {
		af.err = err
		return
	}

	if size != int64(af.committedSize) {
		af.committedSize = uint64(size)
		af.dirty = true
	}

	// back up everything
	// if _, err = af.rf.Seek(0, io.SeekEnd); err != nil {
	// 	af.err = err
	// 	return
	// }

	// for _, node := range af.writtenNodes {
	// 	if node.originalRaw != nil {
	// 		if _, err = af.rf.Write(node.originalRaw); err != nil {
	// 			af.err = err
	// 			return
	// 		}
	// 	}
	// }

	// for _, node := range af.freeNodes {
	// 	if node.dirty && node.originalRaw != nil {
	// 		if _, err = af.rf.Write(node.originalRaw); err != nil {
	// 			af.err = err
	// 			return
	// 		}
	// 	}
	// }

	// if af.dirty {
	// 	if _, err = af.rf.Write(af.originalRaw); err != nil {
	// 		af.err = err
	// 		return
	// 	}
	// }

	// if err = af.rf.Sync(); err != nil {
	// 	af.err = err
	// 	return
	// }

	// write all the changes
	for _, node := range af.writtenNodes {
		if err = node.write(); err != nil {
			af.err = err
			return
		}
	}

	for _, node := range af.freeNodes {
		if node.dirty {
			if err = node.write(); err != nil {
				af.err = err
				return
			}
		}
	}

	if af.dirty {
		if err = af.write(); err != nil {
			af.err = err
			return
		}
	}

	if af.syncEnabled {
		if err = af.f.Sync(); err != nil {
			af.err = err
			return
		}
	}

	// success - discard recovery data
	// if err = af.rf.Truncate(0); err != nil {
	// 	af.err = err
	// 	return
	// }

	// if err = af.rf.Sync(); err != nil {
	// 	af.err = err
	// 	return
	// }

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
