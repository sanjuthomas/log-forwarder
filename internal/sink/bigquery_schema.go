// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package sink

import (
	"fmt"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

func descriptorsFromSchema(schema bigquery.Schema) (protoreflect.MessageDescriptor, *descriptorpb.DescriptorProto, error) {
	convertedSchema, err := adapt.BQSchemaToStorageTableSchema(schema)
	if err != nil {
		return nil, nil, fmt.Errorf("convert table schema: %w", err)
	}

	descriptor, err := adapt.StorageSchemaToProto2Descriptor(convertedSchema, "root")
	if err != nil {
		return nil, nil, fmt.Errorf("convert storage schema to proto: %w", err)
	}

	messageDescriptor, ok := descriptor.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, nil, fmt.Errorf("adapted descriptor is not a message descriptor")
	}

	descriptorProto, err := adapt.NormalizeDescriptor(messageDescriptor)
	if err != nil {
		return nil, nil, fmt.Errorf("normalize descriptor: %w", err)
	}
	return messageDescriptor, descriptorProto, nil
}

func encodeJSONRows(msgDesc protoreflect.MessageDescriptor, payloads [][]byte) ([][]byte, error) {
	if len(payloads) == 0 {
		return nil, nil
	}

	encoded := make([][]byte, len(payloads))
	for i, payload := range payloads {
		msg := dynamicpb.NewMessage(msgDesc)
		if err := protojson.Unmarshal(payload, msg); err != nil {
			return nil, fmt.Errorf("row %d json decode: %w", i, err)
		}
		row, err := proto.Marshal(msg)
		if err != nil {
			return nil, fmt.Errorf("row %d proto encode: %w", i, err)
		}
		encoded[i] = row
	}
	return encoded, nil
}
