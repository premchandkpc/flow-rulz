package handlers

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"gopkg.in/yaml.v3"
)

func readRequestBody(r *http.Request) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBodyBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read request body: %w", err)
	}
	if len(data) > maxRequestBodyBytes {
		return nil, fmt.Errorf("request body exceeds %d bytes", maxRequestBodyBytes)
	}
	return data, nil
}

func isYAMLContentType(contentType string) bool {
	contentType = strings.ToLower(contentType)
	return strings.Contains(contentType, mediaTypeYAML) || strings.Contains(contentType, mediaTypeYML)
}

// decodeSingleYAMLDocument decodes exactly one YAML document and rejects trailing
// documents or malformed trailing content.
func decodeSingleYAMLDocument(data []byte, target any) error {
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode YAML request: %w", err)
	}

	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple YAML documents are not supported")
		}
		return fmt.Errorf("decode trailing YAML document: %w", err)
	}
	return nil
}

// decodeRequestBody decodes one JSON or YAML document. It bounds the input and
// rejects trailing values/multiple YAML documents instead of silently ignoring them.
func decodeRequestBody(r *http.Request, v any) error {
	data, err := readRequestBody(r)
	if err != nil {
		return err
	}
	if isYAMLContentType(r.Header.Get(headerContentType)) {
		return decodeSingleYAMLDocument(data, v)
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(v); err != nil {
		return fmt.Errorf("decode JSON request: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values are not supported")
		}
		return fmt.Errorf("decode trailing JSON value: %w", err)
	}
	return nil
}

// decodeRawRequestBody returns one JSON document, converting YAML documents to
// JSON so application services only need to understand the canonical JSON form.
func decodeRawRequestBody(r *http.Request) (json.RawMessage, error) {
	data, err := readRequestBody(r)
	if err != nil {
		return nil, err
	}
	if !isYAMLContentType(r.Header.Get("Content-Type")) {
		if !json.Valid(data) {
			return nil, errors.New("request body must contain exactly one valid JSON document")
		}
		return json.RawMessage(data), nil
	}

	var document any
	if err := decodeSingleYAMLDocument(data, &document); err != nil {
		return nil, err
	}
	normalized, err := json.Marshal(document)
	if err != nil {
		return nil, fmt.Errorf("convert YAML request to JSON: %w", err)
	}
	return normalized, nil
}
