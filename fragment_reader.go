package uploader

import (
	"io"
)

// fragmentReader implements both Reader
type fragmentReader struct {
	fragments [][]byte
	fragment  uint64
	index     uint64
	delimiter []byte
	pending   []byte
}

// newFragmentReader constructs a fragment reader from the given fragments and
// a delimiter.
// precondition: fragments is not empty
func newFragmentReader(fragments [][]byte, delimiter []byte) io.Reader {
	return &fragmentReader{
		fragments: fragments,
		fragment:  0,
		index:     0,
		delimiter: delimiter,
	}
}

// drained returns true when there is no more data to read.
func (r *fragmentReader) drained() bool {
	return r.fragment >= uint64(len(r.fragments)) && len(r.pending) == 0
}

// advance moves the internal pointer by n bytes. It does not interact with the
// delimiter or pending delimiter data.
// precondition: n does not exceed the remaining size of the current fragment
func (r *fragmentReader) advance(n uint64) (rollover bool) {
	if n+r.index == uint64(len(r.fragments[r.fragment])) {
		r.index = 0
		r.fragment++
		return true
	}
	r.index += n
	return false
}

// eof returns io.EOF if the reader is drained.
func (r *fragmentReader) eof() error {
	if r.drained() {
		return io.EOF
	}
	return nil
}

// writePending writes as many of the pending bytes to p as it can hold.
func (r *fragmentReader) writePending(p []byte) ([]byte, int) {
	if len(r.pending) > 0 {
		n := copy(p, r.pending)
		r.pending = r.pending[n:]
		return p[n:], n
	}
	return p, 0
}

// Read writes as many bytes as are readily available to p. For implementation
// simplicity, it only writes at most one fragment's worth of data per Read
// call (plus up to two delimiters).
func (r *fragmentReader) Read(p []byte) (int, error) {
	// This handles the case that we received zero data, and serves as additional
	// safety belts if Read were to get called after EOF.
	if r.drained() {
		return 0, io.EOF
	}
	p, n := r.writePending(p)
	actual := copy(p, r.fragments[r.fragment][r.index:])
	p = p[actual:]
	n += actual
	if r.advance(uint64(actual)) {
		r.pending = r.delimiter
		_, k := r.writePending(p)
		n += k
	}
	return n, r.eof()
}
