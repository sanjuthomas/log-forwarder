// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package parser

import (
	"strings"
	"testing"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/sanjuthomas/log-forwarder/internal/watcher"
)

func TestLineParserEmitsOneEventPerLine(t *testing.T) {
	t.Parallel()

	p, err := New(config.ParserConfig{Type: "line"})
	if err != nil {
		t.Fatal(err)
	}

	events, err := p.Feed(watcher.LineEvent{
		Path:   "/tmp/app.log",
		Line:   []byte("hello"),
		Offset: 5,
		Inode:  1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("len(events) = %d, want 1", len(events))
	}
	if string(events[0].Data) != "hello" {
		t.Fatalf("data = %q", events[0].Data)
	}
}

func TestMultilineParserGroupsStackTrace(t *testing.T) {
	t.Parallel()

	p, err := New(config.ParserConfig{
		Type:         "multiline",
		StartPattern: `^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}`,
	})
	if err != nil {
		t.Fatal(err)
	}

	header := "2026-06-08 10:16:22.901  ERROR 18432 --- [nio-8080-exec-5] c.a.b.controller.PaymentController       : Payment failed"
	lines := []watcher.LineEvent{
		{Path: "/tmp/app.log", Line: []byte(header), Offset: 100, Inode: 9},
		{Path: "/tmp/app.log", Line: []byte("org.springframework.dao.DataIntegrityViolationException: could not execute statement"), Offset: 180, Inode: 9},
		{Path: "/tmp/app.log", Line: []byte("        at org.springframework.orm.jpa.vendor.HibernateJpaDialect.convertHibernateAccessException(HibernateJpaDialect.java:290)"), Offset: 280, Inode: 9},
		{Path: "/tmp/app.log", Line: []byte("Caused by: org.postgresql.util.PSQLException: ERROR: constraint"), Offset: 360, Inode: 9},
	}

	var published []Event
	for _, line := range lines {
		events, err := p.Feed(line)
		if err != nil {
			t.Fatal(err)
		}
		published = append(published, events...)
	}

	remaining, err := p.Flush()
	if err != nil {
		t.Fatal(err)
	}
	published = append(published, remaining...)

	if len(published) != 1 {
		t.Fatalf("len(published) = %d, want 1", len(published))
	}
	if published[0].Offset != 360 {
		t.Fatalf("offset = %d, want 360", published[0].Offset)
	}

	parts := strings.Split(string(published[0].Data), "\n")
	if len(parts) != 4 {
		t.Fatalf("len(parts) = %d, want 4", len(parts))
	}
	if parts[0] != header {
		t.Fatalf("header = %q", parts[0])
	}
}

func TestMultilineParserFlushesOnNextHeader(t *testing.T) {
	t.Parallel()

	p, err := New(config.ParserConfig{
		Type:         "multiline",
		StartPattern: `^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}`,
	})
	if err != nil {
		t.Fatal(err)
	}

	first, err := p.Feed(watcher.LineEvent{
		Path:   "/tmp/app.log",
		Line:   []byte("2026-06-08 10:00:00.000  INFO 1 --- [main] c.a.App : first"),
		Offset: 50,
		Inode:  1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 0 {
		t.Fatalf("len(first) = %d, want 0", len(first))
	}

	second, err := p.Feed(watcher.LineEvent{
		Path:   "/tmp/app.log",
		Line:   []byte("2026-06-08 10:00:01.000  INFO 1 --- [main] c.a.App : second"),
		Offset: 100,
		Inode:  1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 1 {
		t.Fatalf("len(second) = %d, want 1", len(second))
	}
	if !strings.Contains(string(second[0].Data), "first") {
		t.Fatalf("data = %q, want first event", second[0].Data)
	}

	remaining, err := p.Flush()
	if err != nil {
		t.Fatal(err)
	}
	if len(remaining) != 1 {
		t.Fatalf("len(remaining) = %d, want 1", len(remaining))
	}
	if !strings.Contains(string(remaining[0].Data), "second") {
		t.Fatalf("data = %q, want second event", remaining[0].Data)
	}
}

func TestMultilineParserOrphanLine(t *testing.T) {
	t.Parallel()

	p, err := New(config.ParserConfig{
		Type:         "multiline",
		StartPattern: `^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}`,
	})
	if err != nil {
		t.Fatal(err)
	}

	events, err := p.Feed(watcher.LineEvent{
		Path:   "/tmp/app.log",
		Line:   []byte("        at com.example.App.main(App.java:42)"),
		Offset: 10,
		Inode:  1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("len(events) = %d, want 1", len(events))
	}
}
