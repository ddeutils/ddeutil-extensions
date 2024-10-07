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

### Models

The Model objects was implemented from the [Pydantic V2](https://docs.pydantic.dev/latest/)
which is the powerful parsing and serializing data model to the Python object.

> [!NOTE]
> So, I use this project to learn and implement a limit and trick of the Pydantic
> package.

The model able to handle common logic validations and able to adjust by custom code
for your specific requirements (Yeah, it just inherits Sub-Class from `BaseModel`).

#### Data Types

```python
from ddeutil.vendors.models.dtype import StringType

dtype = StringType()
assert dtype.type == "string"
assert dtype.max_length == -1
```

#### Constraints

```python
from ddeutil.vendors.models.const import Pk

const = Pk(of="foo", cols=["bar", "baz"])
assert const.name == "foo_bar_baz_pk"
assert const.cols == ["bar", "baz"]
```

#### Datasets

```python
from ddeutil.vendors.models.datasets import Col, Tbl

tbl = Tbl(
  name="table_foo",
  features=[
    Col(name="id", dtype="integer primary key"),
    Col(name="foo", dtype="varchar( 10 )"),
  ],
)
assert tbl.name == "table_foo"
assert tbl.features[0].name == "id"
assert tbl.features[0].dtype.type == "integer"
```

## :beers: Usage

### Models

If I have some catalog config, it easy to pass this config to model object.

```python
import yaml
from ddeutil.vendors.models.datasets import Scm


config = yaml.safe_load("""
name: "warehouse"
tables:
  - name: "customer_master"
    features:
      - name: "id"
        dtype: "integer"
        pk: true
      - name: "name"
        dtype: "varchar( 256 )"
        nullable: false
""")
schema = Scm.model_validate(config)
assert len(schema.tables) == 1
assert schema.tables[0].name == 'customer_master'
```
