# Connection

### Connection

The connection for worker able to do anything.

```yaml
conn_postgres_data:
  type: conn.Postgres
  url: 'postgres//username:${ENV_PASS}@hostname:port/database?echo=True&time_out=10'
```

```python
from ddeutil.extensions.conn import Conn

conn = Conn.from_loader(name='conn_postgres_data', externals={})
assert conn.ping()
```

### Dataset

The dataset is defined any objects on the connection. This feature was implemented
on `/extensions` because it has a lot of tools that can interact with any data systems
in the data tool stacks.

```yaml
ds_postgres_customer_tbl:
  type: dataset.PostgresTbl
  conn: 'conn_postgres_data'
  features:
    id: serial primary key
    name: varchar( 100 ) not null
```

```python
from ddeutil.extensions.datasets.pg import PostgresTbl

dataset = PostgresTbl.from_loader(name='ds_postgres_customer_tbl', externals={})
assert dataset.exists()
```
