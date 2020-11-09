// Copyright 2015 The yuyi Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package chunk

import (
	"hash/crc32"
	"io"
	"os"

	"github.com/golang/snappy"
)

type crc32Writer struct {
	writer io.Writer
}

func (w *crc32Writer) Write(p []byte) (n int, err error) {
	chechsum := crc32.ChecksumIEEE(p)
	p = append(p, byte(chechsum>>24), byte(chechsum>>16), byte(chechsum>>8), byte(chechsum))
	return w.writer.Write(p)
}

type snappyWriter struct {
	writer io.Writer
}

func (w *snappyWriter) Write(p []byte) (n int, err error) {
	block := snappy.Encode(nil, p)
	return w.writer.Write(block)
}

type fileWriter struct {
	file string
}

func (w *fileWriter) Write(p []byte) (n int, err error) {
	var f *os.File
	f, err = os.OpenFile(w.file, os.O_WRONLY|os.O_APPEND, os.ModePerm)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	n, err = f.Write(p)
	if err1 := f.Close(); err == nil {
		err = err1
	}
	if err != nil {
		return 0, err
	}
	return n, nil
}
