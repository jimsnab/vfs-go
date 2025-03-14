package vfs

import (
	"errors"
	"os"
	"strings"

	"github.com/jimsnab/afero"
)

func createOrOpenFile(filePath string, forRead bool) (f afero.File, err error) {
	if !forRead {
		f, err = AppFs.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0644)
	}
	if forRead || errors.Is(err, os.ErrExist) {
		f, err = AppFs.OpenFile(filePath, os.O_RDWR|os.O_EXCL, 0644)
	}
	return
}

func isFileNotFound(err error) bool {
	errText := err.Error()
	return strings.Contains(errText, os.ErrNotExist.Error()) || strings.Contains(errText, "no such file or directory") || strings.Contains(errText, "stale file handle")
}
