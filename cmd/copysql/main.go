package main

import (
	"fmt"
	"io"
	"os"

	"github.com/mitchellh/ioprogress"
	"github.com/oleh-ozimok/copysql/pkg/config"
	"github.com/oleh-ozimok/copysql/pkg/datasource"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"

	_ "github.com/oleh-ozimok/copysql/pkg/datasource/clickhouse"
	_ "github.com/oleh-ozimok/copysql/pkg/datasource/vertica"
)

type options struct {
	configFile string
	query      string
}

func main() {
	opts := &options{}
	command := &cobra.Command{
		Use:   "copysql SOURCE DESTINATION TABLE",
		Short: "",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) < 3 {
				return errors.New("invalid number of arguments")
			}

			cfg, err := config.ReadFromFile(opts.configFile)
			if err != nil {
				return errors.Wrap(err, "read configuration failed")
			}

			sourceConfig, err := cfg.GetDataSourceConfig(args[0])
			if err != nil {
				return err
			}

			source, err := datasource.Create(sourceConfig.Driver, sourceConfig.Parameters)
			if err != nil {
				return errors.Wrap(err, "source configuration failed")
			}

			if err := source.Open(); err != nil {
				return errors.Wrap(err, "source connection failed")
			}

			defer source.Close()

			destinationConfig, err := cfg.GetDataSourceConfig(args[1])
			if err != nil {
				return err
			}

			destination, err := datasource.Create(destinationConfig.Driver, destinationConfig.Parameters)
			if err != nil {
				return errors.Wrap(err, "destination configuration failed")
			}

			if err := destination.Open(); err != nil {
				return errors.Wrap(err, "source connection failed")
			}

			defer destination.Close()

			query := opts.query
			if query == "" {
				query = "SELECT * FROM " + args[2]
			}

			pr, pw := io.Pipe()

			var eg errgroup.Group

			eg.Go(func() error {
				defer pw.Close()

				return source.CopyTo(pw, query)
			})

			eg.Go(func() error {
				defer pr.Close()

				progressReader := &ioprogress.Reader{
					Reader: pr,
					DrawFunc: ioprogress.DrawTerminalf(os.Stdout, func(progress int64, _ int64) string {
						return fmt.Sprintf("copied %s", ioprogress.ByteUnitStr(progress))
					}),
				}

				return destination.CopyFrom(progressReader, args[2])
			})

			return eg.Wait()
		},
	}

	command.Flags().StringVarP(&opts.configFile, "config", "c", "config.yaml", "Path to config file")
	command.Flags().StringVarP(&opts.query, "query", "q", "", "Custom select query")

	if err := command.Execute(); err != nil {
		os.Exit(1)
	}
}
