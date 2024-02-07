package vfs

import (
	"errors"
	"os"

	"github.com/jimsnab/afero"
)

func openFile(filePath string) (f afero.File, err error) {
	f, err = AppFs.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0644)
	if errors.Is(err, os.ErrExist) {
		f, err = AppFs.OpenFile(filePath, os.O_RDWR|os.O_EXCL, 0644)
	}
	return
}
