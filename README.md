# Copysql
Copy data between databases (Vertica, Clickhouse)

## Copy data from Vertica to ClickHouse
```
copysql my-vertica my-clickhouse default.my_table -q="SELECT * FROM default.my_table"

```
