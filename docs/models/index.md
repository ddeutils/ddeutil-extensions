# Models

The Model objects was implemented from the [Pydantic V2](https://docs.pydantic.dev/latest/)
which is the powerful parsing and serializing data model to the Python object.

> [!NOTE]
> So, I use this project to learn and implement a limit and trick of the Pydantic
> package.

The model able to handle common logic validations and able to adjust by custom code
for your specific requirements (Yeah, it just inherits Sub-Class from `BaseModel`).

## Data Types

```python
from ddeutil.vendors.models.dtype import StringType

dtype = StringType()
assert dtype.type == "string"
assert dtype.max_length == -1
```

## Constraints

```python
from ddeutil.vendors.models.const import Pk

const = Pk(of="foo", cols=["bar", "baz"])
assert const.name == "foo_bar_baz_pk"
assert const.cols == ["bar", "baz"]
```

## Datasets

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
