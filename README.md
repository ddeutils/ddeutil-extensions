# Extensions

[![size](https://img.shields.io/github/languages/code-size/ddeutils/ddeutil-extensions)](https://github.com/ddeutils/ddeutil-extensions)
[![gh license](https://img.shields.io/github/license/ddeutils/ddeutil-extensions)](https://github.com/ddeutils/ddeutil-extensions/blob/main/LICENSE)
[![code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

An **extensions functions and objects** which provides all plug-ins and objects
that use for data processing and transformation.

**This Extension Focus:**

- Data Transform function with DataFrame API like _Polars_, _Pandas_, or _Duckdb_
- Connection interface like SFTP, Database, No-SQL system, or Cloud services
- Model Datasets

## :round_pushpin: Installation

```shell
pip install -U ddeutil-extensions
```

## :rocket: Features

This extensions package provides 3 main components:

- Plug-in the extension tasks that use with the [Workflow](https://github.com/ddeutils/ddeutil-extensions)
- Connection and Dataset interface objects
- Schema models

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

## :speech_balloon: Contribute

I do not think this project will go around the world because it has specific propose,
and you can create by your coding without this project dependency for long term
solution. So, on this time, you can open [the GitHub issue on this project :raised_hands:](https://github.com/ddeutils/ddeutil-extensions/issues)
for fix bug or request new feature if you want it.
