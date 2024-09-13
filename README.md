# Vendors Data Computing

[![size](https://img.shields.io/github/languages/code-size/ddeutils/ddeutil-vendors)](https://github.com/ddeutils/ddeutil-vendors)
[![gh license](https://img.shields.io/github/license/ddeutils/ddeutil-vendors)](https://github.com/ddeutils/ddeutil-vendors/blob/main/LICENSE)
[![code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

This **Utility Vendors** data computing objects that implement connection and
dataset objects that will use the vendor API like `polars`, `deltalake`, etc,
package to be interface object.

> [!NOTE]
> This project will define the main propose and future objective soon. I think I
> want to create the simplest package that allow me to use it for data transformation
> & data quality with declarative template.

## Installation

This package does not publish with this name yet.

```shell
pip install ddeutil-vendor
```

## Features

### Connection

The connection for worker able to do any thing.

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

The dataset is define any objects on the connection. This feature was implemented
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
from ddeutil.vendors.vendors.pg import PostgresTbl

dataset = PostgresTbl.from_loader(name='ds_postgres_customer_tbl', externals={})
assert dataset.exists()
```

## Usage

```yaml
dq-some-data-domain:
  type: dq.Postgres
  assets:
    - source: <schema>.<table>
      query: |
        ...
```
