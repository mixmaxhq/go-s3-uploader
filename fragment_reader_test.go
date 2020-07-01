package uploader

import (
	"io/ioutil"
	"testing"

	td "github.com/maxatome/go-testdeep/td"
)

var (
	alphabet = "abcdefghijklmnopqrstuvwxyz"
)

func strOfLen(l int) string {
	v := ""
	for len(v) < l {
		v += alphabet
	}
	return v[:l]
}

func TestReader(t *testing.T) {
	blobs := [][]byte{
		[]byte("hello"),
		[]byte("world"),
	}

	blob, err := ioutil.ReadAll(newFragmentReader(blobs, []byte{}))
	if td.CmpNoError(t, err) {
		td.Cmp(t, blob, []byte("helloworld"))
	}
}

func TestReaderDelimiter(t *testing.T) {
	blobs := [][]byte{
		[]byte("{}"),
		[]byte("hflkjlksjdf"),
	}

	blob, err := ioutil.ReadAll(newFragmentReader(blobs, []byte("\n")))
	if td.CmpNoError(t, err) {
		td.Cmp(t, blob, []byte("{}\nhflkjlksjdf\n"))
	}
}

func TestDefaultBoundary(t *testing.T) {
	// ReadAll passes a 512-byte blob to Read by default.
	for l := 508; l < 520; l++ {
		s := strOfLen(l)
		blobs := [][]byte{
			[]byte(s),
			[]byte("asdf"),
		}
		td.Cmp(t, len(s), l)

		blob, err := ioutil.ReadAll(newFragmentReader(blobs, []byte("\n")))
		if td.CmpNoError(t, err) {
			td.Cmp(t, blob, []byte(s+"\nasdf\n"))
		}
	}
}

func TestEmptyFragments(t *testing.T) {
	td.CmpPanic(t,
		func() { newFragmentReader([][]byte{}, []byte("\n")) },
		td.Contains("fragments"))

	blobs := [][]byte{
		[]byte{},
		[]byte{},
		[]byte("a"),
		[]byte{},
	}

	blob, err := ioutil.ReadAll(newFragmentReader(blobs, []byte(".")))
	if td.CmpNoError(t, err) {
		td.Cmp(t, blob, []byte("..a.."))
	}
}

func TestNilDelimiter(t *testing.T) {
	blobs := [][]byte{
		[]byte{},
		[]byte{},
		[]byte("a"),
		[]byte{},
	}

	blob, err := ioutil.ReadAll(newFragmentReader(blobs, nil))
	if td.CmpNoError(t, err) {
		td.Cmp(t, blob, []byte("a"))
	}
}
