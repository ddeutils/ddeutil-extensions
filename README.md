# Vendors

[![size](https://img.shields.io/github/languages/code-size/ddeutils/ddeutil-vendors)](https://github.com/ddeutils/ddeutil-vendors)
[![gh license](https://img.shields.io/github/license/ddeutils/ddeutil-vendors)](https://github.com/ddeutils/ddeutil-vendors/blob/main/LICENSE)
[![code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

A **vendors plugin functions and objects**.

## Installation

This package does not publish with this name yet.

```shell
pip install -U ddeutil-vendor
```

## Features

This vendors package provides 3 main components:

- Plug-in tasks that use with the Workflow
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
from ddeutil.vendors.conn import Conn

conn = Conn.from_loader(name='conn_postgres_data', externals={})
assert conn.ping()
```

### Dataset

The dataset is defined any objects on the connection. This feature was implemented
on `/vendors` because it has a lot of tools that can interact with any data systems
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
from ddeutil.vendors.plugins.pg import PostgresTbl

dataset = PostgresTbl.from_loader(name='ds_postgres_customer_tbl', externals={})
assert dataset.exists()
```

## :speech_balloon: Contribute

I do not think this project will go around the world because it has specific propose,
and you can create by your coding without this project dependency for long term
solution. So, on this time, you can open [the GitHub issue on this project :raised_hands:](https://github.com/ddeutils/ddeutil-vendors/issues)
for fix bug or request new feature if you want it.
