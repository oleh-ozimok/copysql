package clickhouse

import (
	"io"

	"github.com/mitchellh/mapstructure"
	"github.com/oleh-ozimok/go-clickhouse"

	"github.com/oleh-ozimok/copysql/pkg/datasource"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/pkg/errors"
)

const driverName = "clickhouse"

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
	cluster *clickhouse.Cluster
}

func FromParameters(parameters map[string]interface{}) (datasource.Driver, error) {
	params := DriverParameters{}

	if err := mapstructure.Decode(parameters, &params); err != nil {
		return nil, err
	}

	return New(params), nil
}

func New(params DriverParameters) *Driver {
	dsn := "http://" + params.Username + ":" + params.Password + "@" + params.Address

	return &Driver{
		cluster: clickhouse.NewCluster(clickhouse.NewConn(dsn, clickhouse.NewHttpTransport(32))),
	}
}

func (d *Driver) Open() error {
	d.cluster.Check()

	if d.cluster.IsDown() {
		return errors.New("all clickhouse hosts down")
	}

	return nil
}

func (d *Driver) CopyFrom(r io.Reader, table string) error {
	query := clickhouse.BuildCSVInsert(table, r)
	return query.Exec(d.cluster.ActiveConn())
}

func (*Driver) CopyTo(w io.Writer, query string) error {
	panic("not implemented")
}

func (d *Driver) Close() error {
	return nil
}
