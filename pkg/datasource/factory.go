package datasource

var dataSourceFactories = make(map[string]Factory)

type Factory interface {
	Create(parameters map[string]interface{}) (Driver, error)
}

func Register(name string, factory Factory) {
	if _, registered := dataSourceFactories[name]; registered {
		panic("data source factory named " + name + " already registered")
	}

	dataSourceFactories[name] = factory
}

func Create(name string, parameters map[string]interface{}) (Driver, error) {
	dataSourceFactory, ok := dataSourceFactories[name]
	if !ok {
		return nil, InvalidDataSourceError{name}
	}
	return dataSourceFactory.Create(parameters)
}

type InvalidDataSourceError struct {
	Name string
}

func (e InvalidDataSourceError) Error() string {
	return "data source not registered: " + e.Name
}
