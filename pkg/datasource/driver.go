package datasource

import "io"

type Driver interface {
	Open() error
	CopyFrom(r io.Reader, table string) error
	CopyTo(w io.Writer, query string) error
	Close() error
}