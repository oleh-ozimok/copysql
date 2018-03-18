package config

import (
	"io/ioutil"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
)

type Config struct {
	DataSources map[string]*DataSourceConfig
}

type DataSourceConfig struct {
	Driver     string
	Parameters map[string]interface{}
}

func New() *Config {
	return &Config{}
}

func ReadFromFile(filename string) (*Config, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	config := New()

	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, err
	}

	return config, nil
}

func (c *Config) GetDataSourceConfig(name string) (*DataSourceConfig, error) {
	if ds, ok := c.DataSources[name]; ok {
		return ds, nil
	}
	return nil, errors.Errorf("data source %s not found", name)
}
