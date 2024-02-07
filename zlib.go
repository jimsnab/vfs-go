package vfs

import (
	"bytes"
	"compress/zlib"
	"io"
)

var enableCompression = true

func uncompress(compressed []byte) (plain []byte, err error) {
	if enableCompression {
		r := bytes.NewReader(compressed)
		var z io.ReadCloser
		z, err = zlib.NewReader(r)
		if err != nil {
			return
		}
		defer z.Close()

		return io.ReadAll(z)
	} else {
		plain = compressed
	}
	return
}

func compress(plain []byte) (outbound []byte, err error) {
	if enableCompression {
		var buf bytes.Buffer
		z := zlib.NewWriter(&buf)

		_, err = z.Write(plain)
		z.Close()
		if err != nil {
			return
		}

		outbound = buf.Bytes()
	} else {
		outbound = plain
	}
	return
}
