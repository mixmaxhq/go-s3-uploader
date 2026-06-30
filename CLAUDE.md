# go-s3-uploader — repo card

> A map, not a manual. Keep it ~1 screen; point to detail, don't inline it.

## What it is
A Go library (`github.com/mixmaxhq/go-s3-uploader`) that batches byte-array blobs from a channel, joins them with a delimiter, gzip-compresses each batch, and uploads it as a single S3 object. Designed for high-throughput event/log pipelines where minimising S3 PUT count matters.

## serves
role: shared Go library for concurrent, batched, compressed S3 uploads
referenced-by: [<fill: Mixmax services that batch-upload blobs/events to S3 — e.g. sequences, mail, files pipelines>]

## Code map
- Main API        -> `uploader.go` (`Upload(UploadOptions) (<-chan error, error)`)
- Fragment reader  -> `fragment_reader.go` (internal `io.Reader` that interleaves blobs with a delimiter)
- Tests           -> `uploader_test.go`, `fragment_reader_test.go`

## Conventions
- Uses `aws-sdk-go` **v1** (`github.com/aws/aws-sdk-go`) — not v2; callers must supply a pre-configured `*s3.S3` or any `PutClient` interface.
- Content-Type is hardcoded to `application/x-ndjson` + `Content-Encoding: gzip`; the library is opinionated toward NDJSON payloads.
- `GetKey func(time.Time) string` is **required** — callers control the S3 key; omitting it returns an init error.
- `BatchSizeBytes` is a soft limit: the uploader overshoots by at most one blob.

## Gotchas
- `ConcurrentUploads` controls both parallelism and buffer pool size; high values multiply static memory by `ConcurrentUploads × BatchMaxBlobs × 8 bytes`.
- Errors are emitted on the returned channel, not returned directly; drain the channel or you will leak goroutines.
- `BatchWindow` timeout starts when the **first blob** of a batch arrives, not when `Upload` is called.

## Run / test
```bash
go test ./...
```
No build artifact or binary — this is an importable library only.

## Load the matching domain card
This repo is cross-cutting infra/lib — it owns no product domain, so there is no domain card to load. When working here, load the card of the consuming service/domain if the change is driven by its needs.
