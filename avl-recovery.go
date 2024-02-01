package vfs

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

func (tree *avlTree) recover() (err error) {
	f := tree.f
	rf := tree.rf

	fileSize, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return
	}

	if _, err = rf.Seek(0, io.SeekStart); err != nil {
		return
	}

	writes := make(map[uint64]struct{}, 1024)

	var indexSize int64

	for {
		raw := make([]byte, kRecordSize+8)
		n, terr := rf.Read(raw)
		if terr != nil && !errors.Is(terr, io.EOF) {
			err = terr
			return
		}

		if n <= 0 {
			break
		}

		if n < len(raw) {
			err = fmt.Errorf("recovery record length %d is too small", n)
			return
		}

		offset := binary.BigEndian.Uint64(raw[:8])
		if offset > uint64(fileSize) {
			err = fmt.Errorf("recovery record offset beyond index file size: %d", offset)
			return
		}
		if (offset % kRecordSize) != 0 {
			err = errors.New("recovery record offset is not aligned")
			return
		}

		if _, recovered := writes[offset]; recovered {
			// don't overwrite the oldest backup
			continue
		}
		writes[offset] = struct{}{}

		if _, err = f.WriteAt(raw[8:], int64(offset)); err != nil {
			return
		}

		if offset == 0 {
			// this is the replacement header - it informs us what the index file size should be
			var hdr diskHeader
			if err = tree.headerFromRaw(raw[8:], &hdr); err != nil {
				return
			}

			totalRecords := 1 + hdr.NodeCount + hdr.FreeCount
			totalSize := totalRecords * kRecordSize
			if hdr.CommittedSize != totalSize {
				panic(fmt.Sprintf("committed size %d != size of records %d", hdr.CommittedSize, totalSize))
			}

			indexSize = int64(hdr.CommittedSize)
		}
	}

	if indexSize != 0 {
		if err = f.Truncate(indexSize); err != nil {
			return
		}
	}

	if len(writes) > 0 {
		if err = rf.Truncate(0); err != nil {
			return
		}

		if err = f.Sync(); err != nil {
			return
		}
		if err = rf.Sync(); err != nil {
			return
		}
	}

	return
}
