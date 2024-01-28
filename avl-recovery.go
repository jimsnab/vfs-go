package vfs

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

func (af *avlTreeS) recover() (err error) {
	f := af.f
	rf := af.rf

	if _, err = rf.Seek(0, io.SeekStart); err != nil {
		return
	}

	writes := make(map[uint64]struct{}, 1024)

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
		if _, recovered := writes[offset]; recovered {
			// don't overwrite the oldest backup
			continue
		}
		writes[offset] = struct{}{}

		if _, err = f.WriteAt(raw[8:], int64(offset)); err != nil {
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
