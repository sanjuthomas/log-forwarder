// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package config

import (
	"fmt"
	"sort"
	"strings"
)

var (
	sinkTypes        = map[string]struct{}{}
	enricherTypes    = map[string]struct{}{}
	parserTypes      = map[string]struct{}{}
	transformerTypes = map[string]struct{}{}
	filterTypes      = map[string]struct{}{}
)

// RegisterSinkType records a sink type name for config validation. Custom sinks
// should call this from sink.Register; built-in types are pre-registered in init.
func RegisterSinkType(name string) {
	if name == "" {
		return
	}
	sinkTypes[name] = struct{}{}
}

// RegisterEnricherType records an enricher type name for config validation.
func RegisterEnricherType(name string) {
	if name == "" {
		return
	}
	enricherTypes[name] = struct{}{}
}

// RegisterParserType records a parser type name for config validation.
func RegisterParserType(name string) {
	if name == "" {
		return
	}
	parserTypes[name] = struct{}{}
}

// RegisterTransformType records a transformer type name for config validation.
func RegisterTransformType(name string) {
	if name == "" {
		return
	}
	transformerTypes[name] = struct{}{}
}

// RegisterFilterType records a filter predicate type name for config validation.
func RegisterFilterType(name string) {
	if name == "" {
		return
	}
	filterTypes[name] = struct{}{}
}

func knownSinkType(name string) bool {
	_, ok := sinkTypes[name]
	return ok
}

func knownEnricherType(name string) bool {
	_, ok := enricherTypes[name]
	return ok
}

func knownParserType(name string) bool {
	_, ok := parserTypes[name]
	return ok
}

func knownTransformType(name string) bool {
	_, ok := transformerTypes[name]
	return ok
}

func knownFilterType(name string) bool {
	_, ok := filterTypes[name]
	return ok
}

func formatKnownTypes(types map[string]struct{}) string {
	names := make([]string, 0, len(types))
	for name := range types {
		names = append(names, name)
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}

func unknownTypeError(field, value string, types map[string]struct{}) error {
	return fmt.Errorf("%s %q is not registered (known: %s)", field, value, formatKnownTypes(types))
}

func init() {
	for _, name := range []string{"kafka", "file", "http-noauth"} {
		RegisterSinkType(name)
	}
	for _, name := range []string{"static", "host"} {
		RegisterEnricherType(name)
	}
	for _, name := range []string{"line", "multiline"} {
		RegisterParserType(name)
	}
	for _, name := range []string{"delimiter", "tab", "regex"} {
		RegisterTransformType(name)
	}
	for _, name := range []string{"field", "compound"} {
		RegisterFilterType(name)
	}
}
