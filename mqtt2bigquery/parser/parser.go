package parser

import (
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
		fmt.Printf("%#v\n", v)
		return nil, errors.New("invalid type for INTEGER")
	}
}

// Parser parses a JSON value into a BQ type
type Parser interface {
	Parse(interface{}) (bigquery.Value, error)
}

type field struct {
	position int
	parser   Parser
}

// Integer converts a JSON object to []bigquery.Value.
type Record struct {
	names map[string]field
	count int
}

func newRecord() *Record {
	return &Record{names: make(map[string]field)}
}
func (r *Record) Parse(v interface{}) (bigquery.Value, error) {
	return r.ParseRecord(v)
}

func (r *Record) ParseRecord(v interface{}) ([]bigquery.Value, error) {
	m, ok := v.(map[string]interface{})
	if !ok {
		return nil, errors.New("not an object")
	}
	ret := make([]bigquery.Value, r.count)
	var err error
	for k, v := range m {
		if p, ok := r.names[k]; ok {
			if ret[p.position], err = p.parser.Parse(v); err != nil {
				glog.Error(err)
			}
		} else {
			glog.Errorf("No column parser for key %q", k)
		}
	}
	return ret, nil
}

func (r *Record) addField(p Parser, names ...string) {
	f := field{
		parser:   p,
		position: r.count,
	}
	r.count++
	for _, n := range names {
		r.names[n] = f
	}
}

// FromSchema creates a Record parser from a bigquery.Schema. The record parser is configured
// to match JSON keys to column names, case-insensitively, unless the column's description
// in Bigquery contains a struct-like tag resembling `json:"foo"`, in which case "foo" will
// be used as the JSON key to match for this column.
func FromSchema(s bigquery.Schema) (*Record, error) {
	rp := newRecord()
	for _, fs := range s {
		names := []string{fs.Name, strings.ToLower(fs.Name)}
		tags, err := structtag.Parse(fs.Description)
		if err == nil {
			if tag, err := tags.Get("json"); err == nil {
				names = []string{tag.Name}
			}
		}
		switch fs.Type {
		case "STRING":
			rp.addField(String{}, names...)
		case "INTEGER":
			rp.addField(Integer{}, names...)
		case bigquery.TimestampFieldType:
			rp.addField(Timestamp{}, names...)
		default:
			return nil, errors.New("unknown BQ schema type")
		}
	}
	return rp, nil
}
