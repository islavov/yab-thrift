// Copyright (c) 2016 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

// Package thrift contains functionality for converting generic data structures
// to and from Thrift payloads.
package thrift

import (
	"bytes"
	"fmt"
	"go.uber.org/thriftrw/envelope"
	"strings"

	"go.uber.org/thriftrw/compile"
	"go.uber.org/thriftrw/protocol"
	"go.uber.org/thriftrw/wire"
)

// Parse parses the given Thrift file.
func Parse(file string) (*compile.Module, error) {
	module, err := compile.Compile(file, compile.NonStrict())
	// thriftrw wraps errors, so we can't use os.IsNotExist here.
	if err != nil {
		// The user may have left off the ".thrift", so try appending .thrift
		if appendedModule, err2 := compile.Compile(file+".thrift", compile.NonStrict()); err2 == nil {
			module = appendedModule
			err = nil
		}
	}
	return module, err
}

// SplitMethod takes a method name like Service::Method and splits it
// into Service and Method.
func SplitMethod(fullMethod string) (svc, method string, err error) {
	parts := strings.Split(fullMethod, "::")
	switch len(parts) {
	case 1:
		return parts[0], "", nil
	case 2:
		return parts[0], parts[1], nil
	default:
		return "", "", fmt.Errorf("invalid Thrift method %q, expected Service::Method", fullMethod)
	}
}

// ResponseBytesToMap takes the given response bytes and creates a map that
// uses field name as keys.
func RequestBytesToMap(spec *compile.FunctionSpec, requestBytes []byte, opts Options) (map[string]interface{}, error) {
	w, err := requestBytesToWire(requestBytes, opts)
	if err != nil {
		return nil, err
	}

	var specs map[int16]*compile.FieldSpec
	if spec.ArgsSpec != nil {
		specs = getFieldMap(compile.FieldGroup(spec.ArgsSpec))
	}

	result := make(map[string]interface{})
	for _, f := range w.Fields {
		err = nil

		fieldSpec, ok := specs[f.ID]
		if !ok {
			return nil, fmt.Errorf("got unknown exception with ID %v: %v", f.ID, f.Value)
		}

		result[fieldSpec.Name], err = valueFromWire(fieldSpec.Type, f.Value)

		if err != nil {
			return nil, fmt.Errorf("failed to parse result field %v: %v", f.ID, err)
		}
	}

	return result, nil
}

func requestBytesToWire(responseBytes []byte, opts Options) (wire.Struct, error) {
	var w wire.Value
	var err error

	reader := bytes.NewReader(responseBytes)
	if opts.UseEnvelopes {
		w, _, err = envelope.ReadReply(protocol.Binary, bytes.NewReader(responseBytes))
		if err != nil {
			return wire.Struct{}, encodedException{err}
		}
	} else {
		w, err = protocol.Binary.Decode(reader, wire.TStruct)
		if err != nil {
			return wire.Struct{}, fmt.Errorf("cannot parse Thrift struct from response: %v", err)
		}
	}

	if w.Type() != wire.TStruct {
		panic("Got unexpected type when parsing struct")
	}

	return w.GetStruct(), nil
}

// RequestToBytes takes a user request and converts it to the Thrift binary payload.
// It uses the method spec to convert the user request.
func RequestToBytes(method *compile.FunctionSpec, request map[string]interface{}, opts Options) ([]byte, error) {
	w, err := structToValue(compile.FieldGroup(method.ArgsSpec), request)
	if err != nil {
		return nil, err
	}

	buf := &bytes.Buffer{}
	if opts.UseEnvelopes {
		// Sequence IDs are unused, so use the default, 0.
		enveloped := wire.Envelope{
			Name:  opts.EnvelopeMethodPrefix + method.Name,
			Type:  wire.Call,
			Value: wire.NewValueStruct(w),
		}
		err = protocol.Binary.EncodeEnveloped(enveloped, buf)
	} else {
		err = protocol.Binary.Encode(wire.NewValueStruct(w), buf)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to convert Thrift value to bytes: %v", err)
	}

	return buf.Bytes(), nil
}
