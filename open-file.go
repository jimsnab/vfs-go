package vfs

import (
	"errors"
	"io"
	"os"

	"github.com/jimsnab/afero"
)

func createOrOpenFile(filePath string, forRead bool) (f afero.File, err error) {
	fi, terr := AppFs.Stat(filePath)
	size := int64(0)
	if terr == nil {
		size = fi.Size()
	}

	if !forRead {
		f, err = AppFs.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0644)
		if err == nil {
			var end int64
			end, err = f.Seek(0, io.SeekEnd)
			if err != nil {
				return
			}
			if end != size {
				panic("file was truncated!")
			}
		}
	}
	if forRead || errors.Is(err, os.ErrExist) {
		f, err = AppFs.OpenFile(filePath, os.O_RDWR|os.O_EXCL, 0644)
	}
	return
}
