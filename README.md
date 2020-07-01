# go-s3-uploader

Batch, compress and upload blobs into S3 using a configurable bucket and dynamic
prefixes.

Avoids unnecessary copying.

## How to use

```go
package example

import (
  "fmt"
  "time"

  "github.com/aws/aws-sdk-go/aws/session"
  "github.com/aws/aws-sdk-go/service/s3"
  "github.com/google/uuid"
  "github.com/mixmaxhq/go-s3-uploader"
)

// Channel size is arbitrary.
input := make(chan []byte, 64)

sess := session.Must(session.NewSession())

errs := uploader.Upload(uploader.UploadOptions{
  Client: s3.New(sess),
  Bucket: "test-bucket",

  Delimiter: []byte("\n"), // default
  Input:     input,        // required

  ConcurrentUploads: 16, // default

  BatchMaxBlobs:  64,              // default
  BatchSizeBytes: 10485760,        // 10 MiB; disabled (0) by default
  BatchWindow:    time.Minute * 1, // disabled by default

  GetKey: func(start time.Time) {
    // start is the time when we received the first blob in this batch. Note
    // that this may differ from when the blob was inserted into the Input
    // channel due to buffering and scheduling.
    return fmt.Sprintf(
      "/test/%s/%s.ndjson.gz",
      start.Format("2006/01/02/15"),
      uuid.New().String())
  },
})

// Will upload these with newlines to a single S3 object, barring any crazy
// scheduling/contention irregularities.
input <- []byte(`{"key":"value"}`)
input <- []byte(`{"key":"value2"}`)

for err := range errs {
  // Report error.
}

// Upload has finished (usually because Input was closed and drained).
```

Static memory overhead includes 8 bytes per the product of `ConcurrentUploads`
and `BatchMaxBlobs`, so the default values require 8 KiB of overhead. Realistic
use-cases might set `BatchMaxBlobs` to 2<super>17</super>, which means 16 MiB of
static overhead.

The `BatchSizeBytes` isn't a hard limit - the uploader will attempt to meet or
just exceed this value.
