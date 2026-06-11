// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package pipeline

type pendingPublish struct {
	payload []byte
	path    string
	offset  int64
	inode   uint64
}

type publishBuffer struct {
	items []pendingPublish
	bytes int
}

func newPublishBuffer() *publishBuffer {
	return &publishBuffer{}
}

func (b *publishBuffer) len() int {
	return len(b.items)
}

func (b *publishBuffer) bufferedBytes() int {
	return b.bytes
}

func (b *publishBuffer) append(item pendingPublish) {
	b.items = append(b.items, item)
	b.bytes += len(item.payload)
}

func (b *publishBuffer) drain() []pendingPublish {
	if len(b.items) == 0 {
		return nil
	}
	items := b.items
	b.items = nil
	b.bytes = 0
	return items
}
