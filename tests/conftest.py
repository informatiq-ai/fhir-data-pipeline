"""
Pytest configuration and shared fixtures.

Installs a comparable pyspark.sql.types mock into sys.modules so notebook
schema constants can be imported without a Spark runtime.
"""
import sys
import types
from unittest.mock import MagicMock


class _MockType:
    def __eq__(self, other):
        return type(self) is type(other)

    def __hash__(self):
        return hash(type(self).__name__)

    def __repr__(self):
        return f"{type(self).__name__}()"


class StringType(_MockType):
    pass


class IntegerType(_MockType):
    pass


class LongType(_MockType):
    pass


class DoubleType(_MockType):
    pass


class BooleanType(_MockType):
    pass


class TimestampType(_MockType):
    pass


class DateType(_MockType):
    pass


class ArrayType:
    def __init__(self, elementType, containsNull=True):
        self.elementType = elementType

    def __eq__(self, other):
        return isinstance(other, ArrayType) and self.elementType == other.elementType

    def __hash__(self):
        return hash(("ArrayType", self.elementType))

    def __repr__(self):
        return f"ArrayType({self.elementType!r})"


class MapType:
    def __init__(self, keyType, valueType, valueContainsNull=True):
        self.keyType = keyType
        self.valueType = valueType

    def __eq__(self, other):
        return (
            isinstance(other, MapType)
            and self.keyType == other.keyType
            and self.valueType == other.valueType
        )

    def __hash__(self):
        return hash(("MapType", self.keyType, self.valueType))

    def __repr__(self):
        return f"MapType({self.keyType!r}, {self.valueType!r})"


class StructField:
    def __init__(self, name, dataType, nullable=True, metadata=None):
        self.name = name
        self.dataType = dataType
        self.nullable = nullable

    def __eq__(self, other):
        return (
            isinstance(other, StructField)
            and self.name == other.name
            and self.dataType == other.dataType
            and self.nullable == other.nullable
        )

    def __hash__(self):
        return hash((self.name, type(self.dataType).__name__, self.nullable))

    def __repr__(self):
        return f"StructField({self.name!r}, {self.dataType!r}, {self.nullable!r})"


class StructType:
    def __init__(self, fields=None):
        self.fields = list(fields or [])

    def __eq__(self, other):
        return isinstance(other, StructType) and self.fields == other.fields

    def __repr__(self):
        return f"StructType({self.fields!r})"

    def __iter__(self):
        return iter(self.fields)

    def __len__(self):
        return len(self.fields)


_types_module = types.ModuleType("pyspark.sql.types")
for _cls in [
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    BooleanType,
    TimestampType,
    DateType,
    ArrayType,
    MapType,
]:
    setattr(_types_module, _cls.__name__, _cls)

sys.modules.setdefault("pyspark", MagicMock())
sys.modules.setdefault("pyspark.sql", MagicMock())
sys.modules["pyspark.sql.types"] = _types_module
