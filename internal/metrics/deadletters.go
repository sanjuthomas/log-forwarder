// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package metrics

import (
	"encoding/json"
	"net/http"

	"github.com/sanjuthomas/log-forwarder/internal/deadletter"
)

// DeadLetters serves GET /deadletters metadata for on-disk dead letter batches.
type DeadLetters struct {
	Dir string
}

func (d *DeadLetters) enabled() bool {
	return d != nil && d.Dir != ""
}

func (d *DeadLetters) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		entries, err := deadletter.ListEntries(d.Dir)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if entries == nil {
			entries = []deadletter.Entry{}
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(entries)
	}
}
