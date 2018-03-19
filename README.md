# Copysql
Copy data between databases (Vertica, Clickhouse)


## Usage
```
Usage:
  copysql SOURCE DESTINATION TABLE [flags]

Flags:
  -c, --config string   Path to config file (default "config.yaml")
  -h, --help            help for copysql
  -q, --query string    Custom select query

```

## Config file example
``` yaml
datasources:
  my-vertica:
    driver: vertica
    parameters:
      address: vertica.example.com:5433
      username: dbadmin
      password: mypass
      database: default
  my-clickhouse:
    driver: clickhouse
    parameters:
      address: vertica.example.com:8123
      username: default
      password: mypass
      database: default
```

## Usage example
```
copysql my-vertica my-clickhouse default.my_table -c=config.yaml -q="SELECT * FROM default.my_table"

```

## Run in Docker
```

docker run --rm -v /path/to/config.yaml:/config.yaml olegozimok/copysql my-vertica my-clickhouse default.my_table -c=/config.yaml -q="SELECT * FROM default.my_table"

```