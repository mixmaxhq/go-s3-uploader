package uploader

import (
	"errors"
	"fmt"
	"io/ioutil"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
	td "github.com/maxatome/go-testdeep/td"
)

var (
	fixtures = [][]byte{
		[]byte(`{"operation":"decrypt","userId":"5bbb95cbd74eb1172d30c0bd","envelopeId":"eb321c8fa1d569ccd00a70dfcd7fbaed99662caa8ed5fe288d6cb85a04ce15b8","tokenType":"access","service":null,"timestamp":"2020-06-29T22:28:01.730Z","taskRelativeTimestamp":18898.848115,"taskId":"local","eventId":"df03369b-0efb-438c-8646-66ad6ce1e0c4"}`),
		[]byte(`{"operation":"decrypt","userId":"5bbb95cbd74eb1172d30c0bd","envelopeId":"4f6d1e1dc20a4f49d574bb327718f9902fbebceb08325ae52b91ede86645258c","tokenType":"refresh","service":null,"timestamp":"2020-06-29T22:28:01.733Z","taskRelativeTimestamp":18901.033183,"taskId":"local","eventId":"9940f578-1e05-49a0-9753-20117a676f8d"}`),
	}
)

type clientMock struct {
	c           chan int
	objectCount uint64
	objects     []*s3.PutObjectInput
}

func (c *clientMock) PutObject(input *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	// TODO: are there any concerns about shared memory here?
	c.objects = append(c.objects, input)
	atomic.AddUint64(&c.objectCount, 1)
	return nil, nil
}

func config(size uint64, window time.Duration) (*clientMock, chan<- []byte, <-chan error) {
	mock := clientMock{}

	input := make(chan []byte, 16)

	num := uint64(0xffffffffffffffff)

	errs := Upload(UploadOptions{
		Client: &mock,
		Bucket: "test-bucket",

		BatchSizeBytes: size,
		BatchWindow:    window,

		GetKey: func(_ time.Time) string {
			n := atomic.AddUint64(&num, 1)
			return fmt.Sprintf("/test-prefix/%v", n)
		},
		Input: input,
	})

	return &mock, input, errs
}

func TestMisconfigured(tt *testing.T) {
	assert, _ := td.AssertRequire(tt)

	input := make(chan []byte)

	errs := Upload(UploadOptions{
		Client: nil,
		Bucket: "test-bucket",

		GetKey: func(_ time.Time) string {
			return "test-key"
		},
		Input: input,
	})

	err := <-errs
	assert.Cmp(err, errors.New("no client provided"))

	for err := range errs {
		assert.Error(err)
	}
}

func TestUploader(tt *testing.T) {
	assert, require := td.AssertRequire(tt)

	mock, input, errs := config(1024, time.Second*1)

	input <- fixtures[0]
	input <- fixtures[1]
	close(input)

	// Drain.
	for err := range errs {
		assert.Error(err)
	}

	require.Cmp(len(mock.objects), 1)
	obj := mock.objects[0]
	assert.Cmp(*obj.Bucket, "test-bucket")
	assert.Cmp(*obj.Key, "/test-prefix/0")
	if b, err := ioutil.ReadAll(obj.Body); assert.CmpNoError(err) {
		assert.Cmp(b, []byte{
			31, 139, 8, 0, 0, 0, 0, 0, 0, 255, 172, 208, 191, 106, 28, 65, 12, 6, 240,
			222, 143, 161, 250, 230, 144, 230, 175, 102, 223, 32, 109, 184, 42, 221,
			140, 164, 33, 135, 215, 183, 203, 238, 250, 192, 4, 191, 123, 240, 165, 8,
			164, 112, 17, 220, 138, 79, 66, 223, 239, 23, 44, 171, 109, 237, 184, 46,
			55, 152, 64, 77, 182, 183, 245, 128, 19, 188, 238, 182, 125, 83, 152, 32,
			245, 222, 107, 146, 174, 37, 90, 39, 42, 94, 3, 10, 118, 133, 19, 216,
			237, 110, 243, 178, 218, 35, 103, 61, 120, 18, 30, 141, 52, 229, 42, 162,
			136, 173, 160, 14, 209, 50, 122, 51, 173, 53, 103, 47, 173, 177, 105, 26,
			230, 153, 53, 75, 231, 212, 48, 138, 81, 234, 12, 39, 56, 150, 103, 187,
			93, 222, 86, 131, 9, 154, 136, 237, 59, 156, 96, 183, 237, 126, 21, 131,
			233, 246, 58, 207, 39, 56, 174, 47, 182, 31, 237, 101, 133, 9, 60, 122,
			116, 152, 157, 175, 23, 239, 39, 207, 19, 210, 185, 4, 252, 241, 113, 170,
			237, 207, 223, 109, 110, 199, 245, 110, 151, 191, 43, 196, 92, 249, 204,
			145, 137, 210, 159, 208, 227, 247, 121, 145, 54, 127, 20, 186, 219, 237,
			120, 76, 116, 96, 8, 185, 118, 135, 54, 186, 139, 129, 197, 113, 142, 217,
			229, 220, 52, 139, 145, 161, 68, 120, 127, 250, 50, 189, 56, 178, 146,
			145, 138, 199, 22, 71, 172, 154, 74, 236, 61, 248, 82, 136, 71, 173, 232,
			71, 183, 46, 214, 145, 131, 79, 205, 146, 239, 149, 76, 141, 115, 142,
			201, 39, 150, 127, 244, 54, 27, 155, 237, 63, 255, 135, 47, 124, 202, 87,
			145, 206, 24, 2, 113, 248, 148, 175, 214, 136, 35, 21, 118, 100, 152, 92,
			172, 13, 93, 45, 41, 56, 143, 68, 165, 229, 146, 7, 43, 188, 63, 253, 14,
			0, 0, 255, 255, 216, 93, 74, 87, 125, 2, 0, 0,
		})
	}
}

func TestUploaderConcurrent(tt *testing.T) {
	assert, require := td.AssertRequire(tt)

	// The size parameter will force the data into two separate requests.
	mock, input, errs := config(300, time.Second*1)

	input <- fixtures[0]
	input <- fixtures[1]
	close(input)

	// Drain.
	for err := range errs {
		assert.Error(err)
	}

	objs := make(map[string]*s3.PutObjectInput, 2)
	for _, obj := range mock.objects {
		objs[*obj.Key] = obj
	}
	require.Cmp(len(mock.objects), 2)
	assert.Cmp(*objs["/test-prefix/0"].Bucket, "test-bucket")
	assert.Cmp(*objs["/test-prefix/1"].Bucket, "test-bucket")
}

func TestUploaderWindow(tt *testing.T) {
	assert, require := td.AssertRequire(tt)

	// The window parameter will force the data into two separate requests.
	mock, input, errs := config(1024, time.Millisecond*100)

	input <- fixtures[0]

	time.Sleep(100)
	for atomic.LoadUint64(&mock.objectCount) < 1 {
		time.Sleep(10)
	}

	input <- fixtures[1]
	close(input)

	// Drain.
	for err := range errs {
		assert.Error(err)
	}

	require.Cmp(len(mock.objects), 2)

	obj := mock.objects[0]
	assert.Cmp(*obj.Bucket, "test-bucket")
	assert.Cmp(*obj.Key, "/test-prefix/0")

	obj = mock.objects[1]
	assert.Cmp(*obj.Bucket, "test-bucket")
	assert.Cmp(*obj.Key, "/test-prefix/1")
}
