// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"
)

// Readiness evaluates whether the forwarder can accept traffic / do useful work.
type Readiness struct {
	Snapshot         Snapshot
	CheckSink        func(ctx context.Context) error
	IsHibernating    func() bool
	BufferThreshold  float64
	RequireFiles     bool
	SinkCheckEnabled bool
	SinkCheckTimeout time.Duration
}

type readinessResponse struct {
	Status    string `json:"status"`
	ProcessID int    `json:"process_id"`
	Reason    string `json:"reason,omitempty"`
	Detail    string `json:"detail,omitempty"`
}

func (r *Readiness) evaluate(ctx context.Context) readinessResponse {
	if r == nil {
		return readinessResponse{Status: "READY"}
	}

	if r.IsHibernating != nil && r.IsHibernating() {
		return readinessResponse{
			Status: "NOT_READY",
			Reason: "sink_hibernating",
		}
	}

	if r.SinkCheckEnabled && r.CheckSink != nil {
		checkCtx := ctx
		if r.SinkCheckTimeout > 0 {
			var cancel context.CancelFunc
			checkCtx, cancel = context.WithTimeout(ctx, r.SinkCheckTimeout)
			defer cancel()
		}
		if err := r.CheckSink(checkCtx); err != nil {
			return readinessResponse{
				Status: "NOT_READY",
				Reason: "sink_unreachable",
				Detail: err.Error(),
			}
		}
	}

	if r.Snapshot.BufferDepth != nil && r.Snapshot.BufferCapacity > 0 {
		depth := r.Snapshot.BufferDepth()
		threshold := r.bufferThresholdEvents()
		if depth > threshold {
			return readinessResponse{
				Status: "NOT_READY",
				Reason: "pipeline_buffer_high",
				Detail: fmt.Sprintf("buffer depth %d exceeds threshold %d", depth, threshold),
			}
		}
	}

	if r.RequireFiles && r.Snapshot.FilesWatched != nil && r.Snapshot.FilesWatched() == 0 {
		return readinessResponse{
			Status: "NOT_READY",
			Reason: "no_files_watched",
		}
	}

	return readinessResponse{Status: "READY"}
}

func (r *Readiness) bufferThresholdEvents() int64 {
	threshold := r.BufferThreshold
	if threshold <= 0 {
		threshold = 0.8
	}
	return int64(float64(r.Snapshot.BufferCapacity) * threshold)
}

func (r *Readiness) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		result := r.evaluate(req.Context())
		result.ProcessID = os.Getpid()
		w.Header().Set("Content-Type", "application/json")
		if result.Status != "READY" {
			w.WriteHeader(http.StatusServiceUnavailable)
		} else {
			w.WriteHeader(http.StatusOK)
		}
		_ = json.NewEncoder(w).Encode(result)
	}
}
