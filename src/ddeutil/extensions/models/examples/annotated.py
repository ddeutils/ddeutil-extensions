from pprint import pprint
from typing import (
    Annotated,
    Any,
    Callable,
    TypedDict,
    cast,
)

from pydantic import (
    AfterValidator,
    BaseModel,
    BeforeValidator,
    Field,
    PlainValidator,
    ValidationInfo,
    ValidatorFunctionWrapHandler,
    WrapValidator,
)
from pydantic.functional_validators import field_validator, model_validator


class Context(TypedDict):
    logs: list[str]


def make_validator(label: str) -> Callable[[str, ValidationInfo], str]:
    def validator(v: Any, info: ValidationInfo) -> Any:
        ctx = cast(Context, info.context)
        ctx["logs"].append(label)
        return v

    return validator


def make_wrap_validator(
    label: str,
) -> Callable[[str, ValidatorFunctionWrapHandler, ValidationInfo], str]:
    def validator(
        v: Any, handler: ValidatorFunctionWrapHandler, info: ValidationInfo
    ) -> Any:
        ctx = cast(Context, info.context)
        ctx["logs"].append(f"{label}: pre")
        result = handler(v)
        ctx["logs"].append(f"{label}: post")
        return result

    return validator


class A(BaseModel):
    x: Annotated[
        str,
        BeforeValidator(make_validator("x before-1")),
        AfterValidator(make_validator("x after-1")),
        WrapValidator(make_wrap_validator("x wrap-1")),
        BeforeValidator(make_validator("x before-2")),
        AfterValidator(make_validator("x after-2")),
        WrapValidator(make_wrap_validator("x wrap-2")),
        BeforeValidator(make_validator("x before-3")),
        AfterValidator(make_validator("x after-3")),
        WrapValidator(make_wrap_validator("x wrap-3")),
        BeforeValidator(make_validator("x before-4")),
        AfterValidator(make_validator("x after-4")),
        WrapValidator(make_wrap_validator("x wrap-4")),
        Field(description="x"),
    ] = "default x"
    y: Annotated[
        str,
        BeforeValidator(make_validator("y before-1")),
        AfterValidator(make_validator("y after-1")),
        WrapValidator(make_wrap_validator("y wrap-1")),
        BeforeValidator(make_validator("y before-2")),
        AfterValidator(make_validator("y after-2")),
        WrapValidator(make_wrap_validator("y wrap-2")),
        PlainValidator(make_validator("y plain")),
        BeforeValidator(make_validator("y before-3")),
        AfterValidator(make_validator("y after-3")),
        WrapValidator(make_wrap_validator("y wrap-3")),
        BeforeValidator(make_validator("y before-4")),
        AfterValidator(make_validator("y after-4")),
        WrapValidator(make_wrap_validator("y wrap-4")),
        Field(validate_default=False, description="y"),
    ] = "default y"

    val_x_before = field_validator("x", mode="before")(
        make_validator("val_x before")
    )

    val_x_after = field_validator("x", mode="after")(
        make_validator("val_x after")
    )
    val_y_wrap = field_validator("y", mode="wrap")(
        make_wrap_validator("val_y wrap")
    )

    model_before = model_validator(mode="before")(
        make_validator("model before")
    )

    model_after = model_validator(mode="after")(make_validator("model after"))

    model_wrap = model_validator(mode="wrap")(make_wrap_validator("model wrap"))


context = Context(logs=[])

# a = A.model_validate({'x': 'foo', 'y': 'bar'}, context=context)
# assert a.x == 'foo'
# assert a.y == 'bar'
# pprint(context['logs'])

# ['model wrap: pre',
#  'model before',
#  'val_x before',
#  'x wrap-4: pre',
#  'x before-4',
#  'x wrap-3: pre',
#  'x before-3',
#  'x wrap-2: pre',
#  'x before-2',
#  'x wrap-1: pre',
#  'x before-1',
#  'x after-1',
#  'x wrap-1: post',
#  'x after-2',
#  'x wrap-2: post',
#  'x after-3',
#  'x wrap-3: post',
#  'x after-4',
#  'x wrap-4: post',
#  'val_x after',
#  'val_y wrap: pre',
#  'y wrap-4: pre',
#  'y before-4',
#  'y wrap-3: pre',
#  'y before-3',
#  'y plain',
#  'y after-3',
#  'y wrap-3: post',
#  'y after-4',
#  'y wrap-4: post',
#  'val_y wrap: post',
#  'model after',
#  'model wrap: post']

a = A.model_validate({"x": "foo"}, context=context)
assert a.x == "foo"
assert a.y == "default y"
pprint(context["logs"])

# ['model wrap: pre',
#  'model before',
#  'val_x before',
#  'x wrap-4: pre',
#  'x before-4',
#  'x wrap-3: pre',
#  'x before-3',
#  'x wrap-2: pre',
#  'x before-2',
#  'x wrap-1: pre',
#  'x before-1',
#  'x after-1',
#  'x wrap-1: post',
#  'x after-2',
#  'x wrap-2: post',
#  'x after-3',
#  'x wrap-3: post',
#  'x after-4',
#  'x wrap-4: post',
#  'val_x after',
#  'model after',
#  'model wrap: post']
