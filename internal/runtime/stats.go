package runtime

import "sync/atomic"

type Stats struct {
	LinesPublished  atomic.Uint64
	PublishFailures atomic.Uint64
}
