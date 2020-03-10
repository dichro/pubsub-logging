package parser

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/fatih/structtag"
	"github.com/golang/glog"
)

// Timestamp converts a JSON value to a TIMESTAMP.
type Timestamp struct{}

func (Timestamp) Parse(v interface{}) (bigquery.Value, error) {
	if s, ok := v.(string); ok {
		return time.Parse(time.RFC3339Nano, s)
	}
	if n, ok := v.(float64); ok {
		// treat it as a unix timestamp
		return n, nil
	}
	return nil, errors.New("invalid type for TIMESTAMP")
}

// String converts a JSON value to a STRING.
type String struct{}

func (String) Parse(v interface{}) (bigquery.Value, error) {
	return fmt.Sprint(v), nil
}

// Integer converts a JSON value to an INTEGER.
type Integer struct{}

func (Integer) Parse(v interface{}) (bigquery.Value, error) {
	switch n := v.(type) {
	case float64:
		return n, nil
	default:
		return nil, errors.New("invalid type for INTEGER")
	}
}

type Boolean struct{}

func (Boolean) Parse(v interface{}) (bigquery.Value, error) {
	if b, ok := v.(bool); ok {
		return b, nil
	}
	return nil, errors.New("invalid type for BOOLEAN")
}

type Bytes struct{}

func (Bytes) Parse(v interface{}) (bigquery.Value, error) {
	if s, ok := v.(string); ok {
		return base64.StdEncoding.DecodeString(s)
	}
	return nil, errors.New("invalid type for BYTES")
}

// Parser parses a JSON value into a BQ type
type Parser interface {
	Parse(interface{}) (bigquery.Value, error)
}

type Field struct {
	position int
	parser   Parser
}

type Record struct {
	count  int
	fields map[string]Field
}

func (r *Record) ParseAsRecord(v interface{}) (output []bigquery.Value, err error) {
	m, ok := v.(map[string]interface{})
	if !ok {
		return nil, errors.New("not an object")
	}
	output = make([]bigquery.Value, r.count)
	for k, v := range m {
		if p, ok := r.fields[k]; ok {
			if output[p.position], err = p.parser.Parse(v); err != nil {
				glog.Error(err)
			}
		} else {
			glog.Errorf("No column parser for key %q", k)
		}
	}
	return output, nil
}

func (r *Record) Parse(v interface{}) (bigquery.Value, error) {
	return r.ParseAsRecord(v)
}

func (r *Record) addField(parser Parser, index int, names ...string) {
	f := Field{
		position: index,
		parser:   parser,
	}
	for _, n := range names {
		r.fields[n] = f
	}
}

func newRecord(count int) *Record {
	return &Record{
		count:  count,
		fields: make(map[string]Field),
	}
}

// NewRecord creates a Record parser from a bigquery.Schema. The record parser is configured
// to match JSON keys to column names, case-insensitively, unless the column's description
// in Bigquery contains a struct-like tag resembling `json:"foo"`, in which case "foo" will
// be used as the JSON key to match for this column.
func NewRecord(s bigquery.Schema) (*Record, error) {
	root := newRecord(len(s))
	for i, fs := range s {
		names := []string{fs.Name, strings.ToLower(fs.Name)}
		tags, err := structtag.Parse(fs.Description)
		if err == nil {
			if tag, err := tags.Get("json"); err == nil {
				names = []string{tag.Name}
			}
		}
		var p Parser
		switch fs.Type {
		case bigquery.StringFieldType:
			p = String{}
		case bigquery.IntegerFieldType:
			p = Integer{}
		case bigquery.TimestampFieldType:
			p = Timestamp{}
			root.addField(Timestamp{}, i, names...)
		case bigquery.BooleanFieldType:
			p = Boolean{}
		case bigquery.BytesFieldType:
			p = Bytes{}
		case bigquery.RecordFieldType:
			if r, err := NewRecord(fs.Schema); err == nil {
				p = r
			} else {
				return nil, err
			}
		default:
			// return nil, fmt.Errorf("unknown BQ schema type %q", fs.Type)
			glog.Errorf("unknown BQ schema type %q", fs.Type)
			continue
		}
		if fs.Repeated {
			root.addField(&Repeated{p}, i, names...)
		} else {
			root.addField(p, i, names...)
		}
	}
	return root, nil
}

// Repeated wraps a Parser to generate REPEATED column values.
type Repeated struct {
	parser Parser
}

func (r *Repeated) Parse(v interface{}) (bigquery.Value, error) {
	m, ok := v.([]interface{})
	if !ok {
		return make([]bigquery.Value, 0), errors.New("not an array")
	}
	output := make([]bigquery.Value, len(m))
	var err error
	for i, v := range m {
		if output[i], err = r.parser.Parse(v); err != nil {
			return nil, err
		}
	}
	return output, nil
}
