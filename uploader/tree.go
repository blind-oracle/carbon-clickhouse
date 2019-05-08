package uploader

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"io"
	"time"

	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
)

type Tree struct {
	*cached
}

var _ Uploader = &Tree{}
var _ UploaderWithReset = &Tree{}

func NewTree(base *Base) *Tree {
	u := &Tree{}
	u.cached = newCached(base)
	u.cached.parser = u.parseFile
	// Tree table does not have Date column anymore, so override query here
	u.query = fmt.Sprintf("%s (Level, Path, Version)", u.config.TableName)
	return u
}

func (u *Tree) parseFile(filename string, out io.Writer) (map[string]bool, error) {
	reader, err := RowBinary.NewReader(filename, false)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	version := uint32(time.Now().Unix())

	newSeries := make(map[string]bool)

	var level, index, l int
	var p []byte

	writePathLevel := func(p []byte, level int) error {
		if err := RowBinary.WriteUint32(out, uint32(level)); err != nil {
			return err
		}
		if err := RowBinary.WriteBytes(out, p); err != nil {
			return err
		}
		if err := RowBinary.WriteUint32(out, version); err != nil {
			return err
		}
		return nil
	}

LineLoop:
	for {
		name, err := reader.ReadRecord()
		if err != nil { // io.EOF or corrupted file
			break
		}

		// skip tagged
		if bytes.IndexByte(name, '?') >= 0 {
			continue
		}

		h := sha1.Sum(name)
		key := unsafeString(h[:])

		if u.existsCache.Exists(key) {
			continue LineLoop
		}

		if newSeries[key] {
			continue LineLoop
		}

		newSeries[key] = true
		level = pathLevel(name)

		if err = writePathLevel(name, level); err != nil {
			return nil, err
		}

		p = name
		l = level
		for l--; l > 0; l-- {
			index = bytes.LastIndexByte(p, '.')
			if newSeries[unsafeString(p[:index+1])] {
				break
			}

			newSeries[string(p[:index+1])] = true

			if err = writePathLevel(p[:index+1], l); err != nil {
				return nil, err
			}

			p = p[:index]
		}
	}

	return newSeries, nil
}
