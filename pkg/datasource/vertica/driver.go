package vertica

import (
	"database/sql"
	"encoding/csv"
	"io"

	"github.com/mitchellh/mapstructure"
	"github.com/oleh-ozimok/copysql/pkg/datasource"

	_ "github.com/lib/pq"
)

const driverName = "vertica"

func init() {
	datasource.Register(driverName, &detectorFactory{})
}

type detectorFactory struct{}

func (f *detectorFactory) Create(parameters map[string]interface{}) (datasource.Driver, error) {
	return FromParameters(parameters)
}

type DriverParameters struct {
	Address  string
	Username string
	Password string
	Database string
}

type Driver struct {
	dsn        string
	connection *sql.DB
}

func FromParameters(parameters map[string]interface{}) (datasource.Driver, error) {
	params := DriverParameters{}

	if err := mapstructure.Decode(parameters, &params); err != nil {
		return nil, err
	}

	return New(params), nil
}

func New(params DriverParameters) *Driver {
	return &Driver{
		dsn: "postgres://" + params.Username + ":" + params.Password + "@" + params.Address + "/" + params.Database + "?sslmode=disable",
	}
}

func (d *Driver) Open() (err error) {
	d.connection, err = sql.Open("postgres", d.dsn)

	return
}

func (*Driver) CopyFrom(r io.Reader, table string) error {
	panic("not implemented")
}

func (d *Driver) CopyTo(w io.Writer, query string) error {
	rows, err := d.connection.Query(query)
	if err != nil {
		return err
	}

	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	readColumns := make([]interface{}, len(columns))
	writeColumns := make([]sql.NullString, len(columns))

	for i := range writeColumns {
		readColumns[i] = &writeColumns[i]
	}

	csvWriter := csv.NewWriter(w)
	csvWriter.UseCRLF = true

	record := make([]string, len(columns))

	for rows.Next() {
		if err := rows.Scan(readColumns...); err != nil {
			return err
		}

		for i := range writeColumns {
			record[i] = writeColumns[i].String
		}

		csvWriter.Write(record)
	}

	csvWriter.Flush()

	return rows.Err()
}

func (d *Driver) Close() error {
	return d.connection.Close()
}
