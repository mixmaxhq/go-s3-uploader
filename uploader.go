package uploader

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
)

var (
	contentType     = aws.String("application/x-ndjson")
	contentEncoding = aws.String("gzip")

	newlineBytes = []byte("\n")
)

// PutClient represents the subset of the S3 interface we use in the Uploader, which facilitates
// test mocks.
type PutClient interface {
	PutObject(*s3.PutObjectInput) (*s3.PutObjectOutput, error)
	PutObjectWithContext(context.Context, *s3.PutObjectInput, ...request.Option) (*s3.PutObjectOutput, error)
}

// UploadOptions contains the configuration information for the Upload process, and exposes no
// public methods.
type UploadOptions struct {
	// Delimiter separates blobs within the S3 objects. If nil, defaults to '\n'; pass an empty byte
	// array to disable.
	Delimiter []byte
	// Input provides a stream of blobs (byte arrays) to batch and upload to S3.
	Input <-chan []byte

	// Client is a pre-configured S3 client capable of writing objects to the defined S3 Bucket.
	Client PutClient
	// Bucket is the S3 bucket into which batched uploads get saved.
	Bucket string

	// ConcurrentUploads is the number of concurrent S3 object writes we should have going at any one
	// point in time.
	ConcurrentUploads uint
	// BatchMaxBlobs is the maximum number of blobs to batch into a single object. This should
	// probably be pretty high, as BatchSizeBytes is intended to be the primary batching driver.
	BatchMaxBlobs uint
	// BatchSizeBytes is the batch cutoff size, in bytes. All batches that do not time out or run out
	// of buffer space (in slices) will attempt to just exceed this.
	BatchSizeBytes uint64
	// BatchWindow is the batch cutoff duration; when a batch has been collecting for at least this
	// long, it'll be considered complete and uploaded. This should be defined by the amount of data
	// you're willing to lose.
	BatchWindow time.Duration
	// UploadTimeout is the length of time a single upload may take before it gets canceled.
	UploadTimeout time.Duration

	// GetKey generates the key string given the time at which the first blob arrived. It is invoked
	// immediately when a buffer is closed, and used to name/locate the object as it gets uploaded.
	GetKey func(time.Time) string

	// errors is the output channel for emitting errors. These are generally non- fatal, except for
	// any errors that occur in the Upload function itself. This channel closes when the uploader
	// stops - usually when the Input channel was closed and all blobs received have been written to
	// S3.
	errors chan<- error

	// bufferPool is a pool of buffers sized according to ConcurrentUploads. One buffer corresponds to
	// one potential goroutine. These buffers are sized according to BatchMaxBlobs, and store the
	// blobs provided.
	bufferPool chan [][]byte
}

// compress compresses the input reader and buffers the compressed data.
func compress(raw io.Reader) (io.ReadSeeker, error) {
	buf := bytes.Buffer{}
	c := gzip.NewWriter(&buf)
	if _, err := io.Copy(c, raw); err != nil {
		return nil, err
	}
	if err := c.Close(); err != nil {
		return nil, err
	}
	return bytes.NewReader(buf.Bytes()), nil
}

// Batch represents a buffer and the time it started populating.
type Batch struct {
	buffer        [][]byte
	firstBlobTime time.Time
	size          uint64
}

func (u UploadOptions) isBatchFull(b Batch) bool {
	return (u.BatchSizeBytes > 0 && b.size >= u.BatchSizeBytes) || len(b.buffer) == cap(b.buffer)
}

// emitError emits an error in the errors channel.
func (u UploadOptions) emitError(value interface{}, context string) {
	if value == nil {
		return
	}
	if err, ok := value.(error); ok {
		u.errors <- fmt.Errorf("%v: %w", context, err)
	} else {
		u.errors <- errors.New("unspecified error: " + context)
	}
}

// uploadObject runs in a new goroutine and handles uploading a batch of blobs to S3, including
// compressing the data and inserting delimiters.
func (u UploadOptions) uploadObject(batch *Batch) {
	uploadContext := fmt.Sprintf("Upload uploadObject function (%v blobs)", len(batch.buffer))

	defer func() {
		u.emitError(recover(), uploadContext)
	}()

	// Return the buffer to the pool.
	defer func() {
		// Clear the blob references so they can get collected.
		for i := range batch.buffer {
			batch.buffer[i] = nil
		}

		u.bufferPool <- batch.buffer[:0]
	}()

	key := u.GetKey(batch.firstBlobTime)

	reader := newFragmentReader(batch.buffer, newlineBytes)
	compressed, err := compress(reader)
	if err != nil {
		// TODO: consider just uploading the uncompressed data as a fallback?
		u.emitError(fmt.Errorf("compression error: %w", err), uploadContext)
		return
	}

	ctx := context.Background()
	if u.UploadTimeout > time.Second * 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), u.UploadTimeout)
		defer cancel()
	}

	_, err = u.Client.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Body:   compressed,
		Bucket: aws.String(u.Bucket),
		Key:    aws.String(key),

		ContentEncoding: contentEncoding,
		ContentType:     contentType,
	})

	if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == request.CanceledErrorCode {
		u.emitError(fmt.Errorf("request canceled: %w", awsErr), uploadContext)
		return
	}

	u.emitError(err, uploadContext)
}

func noop() {}

// contextWithTimeout is a helper function that only derives a new context with a timeout if the
// BatchWindow is configured.
func (u UploadOptions) contextWithTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if u.BatchWindow == 0 {
		return ctx, noop
	}

	return context.WithTimeout(ctx, u.BatchWindow)
}

// getBatch collects a batch together and returns it and additional metadata. Respects the various
// batch parameters, including BatchSizeBytes, BatchWindow, and BatchMaxBlobs.
func (u UploadOptions) getBatch(runCtx context.Context) *Batch {
	blob, ok := <-u.Input
	if !ok {
		return nil
	}

	b := Batch{
		firstBlobTime: time.Now(),
		size:          uint64(len(blob)),
	}
	b.buffer = <-u.bufferPool
	b.buffer = append(b.buffer, blob)

	if u.isBatchFull(b) {
		return &b
	}

	ctx, cancel := u.contextWithTimeout(runCtx)
	defer cancel()

	for {
		select {
		case blob, ok := <-u.Input:
			if ok {
				b.buffer = append(b.buffer, blob)
				b.size += uint64(len(blob))
				if !u.isBatchFull(b) {
					continue
				}

				// The buffer is full, either in quantity or in accumulated size.
			}

		case <-ctx.Done():
			// Timeout reached; consider the batch complete.
		}

		break
	}

	return &b
}

// run aggregates batches and spins up goroutines to upload the batches as S3 objects. When the
// Input is closed and all uploads are complete, this will drain the buffer pool, close the errors
// channel, and exit.
func (u UploadOptions) run() {
	// Attempt graceful completion even in the event of a panic.
	defer func() {
		// Wait for all uploads to end.
		for i := uint(0); i < u.ConcurrentUploads; i++ {
			<-u.bufferPool
		}

		close(u.errors)
	}()

	defer func() {
		u.emitError(recover(), "Upload run function")
	}()

	ctx := context.Background()

	for {
		batch := u.getBatch(ctx)
		if batch == nil {
			break
		}
		go u.uploadObject(batch)
	}
}

func fatal(e chan error, err error) chan error {
	e <- err
	close(e)
	return e
}

// Upload consumes a channel containing byte arrays (blobs), joins them with a delimiter in batches,
// and uploads the batches to S3. The error channel outputs runtime errors, while the error return
// value outputs initialization errors.
func Upload(options UploadOptions) (<-chan error, error) {
	e := make(chan error, 1)
	options.errors = e

	if options.Client == nil {
		return nil, errors.New("no client provided")
	}

	if options.GetKey == nil {
		return nil, errors.New("no key getter provided")
	}

	if options.Input == nil {
		return nil, errors.New("nil input")
	}

	if options.Delimiter == nil {
		options.Delimiter = []byte("\n")
	}

	if options.ConcurrentUploads == 0 {
		options.ConcurrentUploads = 4
	}

	if options.BatchMaxBlobs == 0 {
		options.BatchMaxBlobs = 64
	}

	options.bufferPool = make(chan [][]byte, options.ConcurrentUploads)
	for i := uint(0); i < options.ConcurrentUploads; i++ {
		options.bufferPool <- make([][]byte, 0, options.BatchMaxBlobs)
	}

	go options.run()

	return e, nil
}
