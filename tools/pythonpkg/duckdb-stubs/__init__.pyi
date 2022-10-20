# to regenerate this from scratch, run scripts/regenerate_python_stubs.sh .
# be warned - currently there are still tweaks needed after this file is
# generated. These should be annotated with a comment like
# # stubgen override
# to help the sanity of maintainers.
from typing import Any, ClassVar

from typing import overload
import pandas
import pyarrow.lib
_clean_default_connection: PyCapsule
apilevel: str
comment: token_type
default_connection: DuckDBPyConnection
identifier: token_type
keyword: token_type
numeric_const: token_type
operator: token_type
paramstyle: str
string_const: token_type
threadsafety: int

class BinderException(ProgrammingError): ...

class CastException(DataError): ...

class CatalogException(ProgrammingError): ...

class ConnectionException(OperationalError): ...

class ConstraintException(IntegrityError): ...

class ConversionException(DataError): ...

class DataError(Error): ...

class DuckDBPyConnection:
    def __init__(self, *args, **kwargs) -> None: ...
    def append(self, table_name: str, df: pandas.DataFrame) -> DuckDBPyConnection: ...
    def arrow(self, chunk_size: int = ...) -> pyarrow.lib.Table: ...
    def begin(self) -> DuckDBPyConnection: ...
    def close(self) -> None: ...
    def commit(self) -> DuckDBPyConnection: ...
    def cursor(self) -> DuckDBPyConnection: ...
    def df(self, df: pandas.DataFrame) -> DuckDBPyRelation: ...
    def duplicate(self) -> DuckDBPyConnection: ...
    def execute(self, query: str, parameters: object = ..., multiple_parameter_sets: bool = ...) -> DuckDBPyConnection: ...
    def executemany(self, query: str, parameters: object = ...) -> DuckDBPyConnection: ...
    def fetch_arrow_table(self, chunk_size: int = ...) -> pyarrow.lib.Table: ...
    def fetch_df(self, *args, **kwargs) -> Any: ...
    def fetch_df_chunk(self, *args, **kwargs) -> Any: ...
    def fetch_record_batch(self, chunk_size: int = ...) -> pyarrow.lib.RecordBatchReader: ...
    def fetchall(self) -> list: ...
    def fetchdf(self, *args, **kwargs) -> Any: ...
    def fetchmany(self, size: int = ...) -> list: ...
    def fetchnumpy(self) -> dict: ...
    def fetchone(self) -> object: ...
    def from_arrow(self, arrow_object: object) -> DuckDBPyRelation: ...
    def from_csv_auto(self, file_name: str) -> DuckDBPyRelation: ...
    def from_df(self, df: pandas.DataFrame = ...) -> DuckDBPyRelation: ...
    def from_parquet(self, file_name: str, binary_as_string: bool = ...) -> DuckDBPyRelation: ...
    def from_query(self, query: str, alias: str = ...) -> DuckDBPyRelation: ...
    def from_substrait(self, proto: bytes) -> DuckDBPyRelation: ...
    def get_substrait(self, query: str) -> DuckDBPyRelation: ...
    def get_substrait_json(self, query: str) -> DuckDBPyRelation: ...
    def get_table_names(self, query: str) -> Set[str]: ...
    def install_extension(self, *args, **kwargs) -> Any: ...
    def load_extension(self, extension: str) -> None: ...
    def query(self, query: str, alias: str = ...) -> DuckDBPyRelation: ...
    def register(self, view_name: str, python_object: object) -> DuckDBPyConnection: ...
    def rollback(self) -> DuckDBPyConnection: ...
    def table(self, table_name: str) -> DuckDBPyRelation: ...
    def table_function(self, name: str, parameters: object = ...) -> DuckDBPyRelation: ...
    def unregister(self, view_name: str) -> DuckDBPyConnection: ...
    def value_rows(self, values: list) -> DuckDBPyRelation: ...
    def values(self, values: object) -> DuckDBPyRelation: ...
    def view(self, view_name: str) -> DuckDBPyRelation: ...
    def __enter__(self) -> DuckDBPyConnection: ...
    def __exit__(self, exc_type: object, exc: object, traceback: object) -> bool: ...
    @property
    def description(self) -> object: ...

class DuckDBPyRelation:
    def __init__(self, *args, **kwargs) -> None: ...
    def abs(self, aggregation_columns: str) -> DuckDBPyRelation: ...
    def aggregate(self, aggr_expr: str, group_expr: str = ...) -> DuckDBPyRelation: ...
    def apply(self, function_name: str, function_aggr: str, group_expr: str = ..., function_parameter: str = ..., projected_columns: str = ...) -> DuckDBPyRelation: ...
    def arrow(self, batch_size: int = ...) -> pyarrow.lib.Table: ...
    def count(self, count_aggr: str, group_expr: str = ...) -> DuckDBPyRelation: ...
    def create(self, table_name: str) -> None: ...
    def create_view(self, view_name: str, replace: bool = ...) -> DuckDBPyRelation: ...
    def cummax(self, aggregation_columns: str) -> DuckDBPyRelation: ...
    def cummin(self, aggregation_columns: str) -> DuckDBPyRelation: ...
    def cumprod(self, aggregation_columns: str) -> DuckDBPyRelation: ...
    def cumsum(self, aggregation_columns: str) -> DuckDBPyRelation: ...
    def describe(self) -> DuckDBPyRelation: ...
    def df(self, *args, **kwargs) -> Any: ...
    def distinct(self) -> DuckDBPyRelation: ...
    def except_(self, other_rel: DuckDBPyRelation) -> DuckDBPyRelation: ...
    def execute(self, *args, **kwargs) -> Any: ...
    def explain(self) -> str: ...
    def fetchall(self) -> object: ...
    def fetchmany(self, size: int = ...) -> object: ...
    def fetchnumpy(self) -> dict: ...
    def fetchone(self) -> object: ...
    def filter(self, filter_expr: str) -> DuckDBPyRelation: ...
    def insert(self, values: object) -> None: ...
    def insert_into(self, table_name: str) -> None: ...
    def intersect(self, other_rel: DuckDBPyRelation) -> DuckDBPyRelation: ...
    def join(self, other_rel: DuckDBPyRelation, condition: str, how: str = ...) -> DuckDBPyRelation: ...
    def kurt(self, aggregation_columns: str, group_columns: str = ...) -> DuckDBPyRelation: ...
    def limit(self, n: int, offset: int = ...) -> DuckDBPyRelation: ...
    def mad(self, aggregation_columns: str, group_columns: str = ...) -> DuckDBPyRelation: ...
    def map(self, map_function: function) -> DuckDBPyRelation: ...
    def max(self, max_aggr: str, group_expr: str = ...) -> DuckDBPyRelation: ...
    def mean(self, mean_aggr: str, group_expr: str = ...) -> DuckDBPyRelation: ...
    def median(self, median_aggr: str, group_expr: str = ...) -> DuckDBPyRelation: ...
    def min(self, min_aggr: str, group_expr: str = ...) -> DuckDBPyRelation: ...
    def mode(self, aggregation_columns: str, group_columns: str = ...) -> DuckDBPyRelation: ...
    def order(self, order_expr: str) -> DuckDBPyRelation: ...
    def prod(self, aggregation_columns: str, group_columns: str = ...) -> DuckDBPyRelation: ...
    def project(self, project_expr: str) -> DuckDBPyRelation: ...
    def quantile(self, q: str, quantile_aggr: str, group_expr: str = ...) -> DuckDBPyRelation: ...
    def query(self, virtual_table_name: str, sql_query: str) -> DuckDBPyRelation: ...
    def record_batch(self, batch_size: int = ...) -> pyarrow.lib.RecordBatchReader: ...
    def sem(self, aggregation_columns: str, group_columns: str = ...) -> DuckDBPyRelation: ...
    def set_alias(self, alias: str) -> DuckDBPyRelation: ...
    def skew(self, aggregation_columns: str, group_columns: str = ...) -> DuckDBPyRelation: ...
    def std(self, std_aggr: str, group_expr: str = ...) -> DuckDBPyRelation: ...
    def sum(self, sum_aggr: str, group_expr: str = ...) -> DuckDBPyRelation: ...
    def to_arrow_table(self, batch_size: int = ...) -> pyarrow.lib.Table: ...
    def to_df(self, *args, **kwargs) -> Any: ...
    def union(self, union_rel: DuckDBPyRelation) -> DuckDBPyRelation: ...
    def unique(self, unique_aggr: str) -> DuckDBPyRelation: ...
    def value_counts(self, value_counts_aggr: str, group_expr: str = ...) -> DuckDBPyRelation: ...
    def var(self, var_aggr: str, group_expr: str = ...) -> DuckDBPyRelation: ...
    def write_csv(self, file_name: str) -> None: ...
    def __len__(self) -> int: ...
    @property
    def alias(self) -> str: ...
    @property
    def columns(self) -> list: ...
    @property
    def dtypes(self) -> list: ...
    @property
    def shape(self) -> tuple: ...
    @property
    def type(self) -> str: ...
    @property
    def types(self) -> list: ...

class DuckDBPyResult:
    def __init__(self, *args, **kwargs) -> None: ...
    def arrow(self, chunk_size: int = ...) -> pyarrow.lib.Table: ...
    def close(self) -> None: ...
    def description(self) -> list: ...
    def df(self, *args, **kwargs) -> Any: ...
    def fetch_arrow_reader(self, approx_batch_size: int) -> pyarrow.lib.RecordBatchReader: ...
    def fetch_arrow_table(self, chunk_size: int = ...) -> pyarrow.lib.Table: ...
    def fetch_df(self, *args, **kwargs) -> Any: ...
    def fetch_df_chunk(self, *args, **kwargs) -> Any: ...
    def fetchall(self) -> list: ...
    def fetchdf(self, *args, **kwargs) -> Any: ...
    def fetchmany(self, size: int = ...) -> list: ...
    def fetchnumpy(self) -> dict: ...
    def fetchone(self) -> object: ...

class Error(Exception): ...

class FatalException(Error): ...

class IOException(OperationalError): ...

class IntegrityError(Error): ...

class InternalError(Error): ...

class InternalException(InternalError): ...

class InterruptException(Error): ...

class InvalidInputException(ProgrammingError): ...

class InvalidTypeException(ProgrammingError): ...

class NotImplementedException(NotSupportedError): ...

class NotSupportedError(Error): ...

class OperationalError(Error): ...

class OutOfMemoryException(OperationalError): ...

class OutOfRangeException(DataError): ...

class ParserException(ProgrammingError): ...

class PermissionException(Error): ...

class ProgrammingError(Error): ...

class SequenceException(Error): ...

class SerializationException(OperationalError): ...

class StandardException(Error): ...

class SyntaxException(ProgrammingError): ...

class TransactionException(OperationalError): ...

class TypeMismatchException(DataError): ...

class ValueOutOfRangeException(DataError): ...

class Warning(Exception): ...

class token_type:
    __members__: ClassVar[dict] = ...  # read-only
    __entries: ClassVar[dict] = ...
    comment: ClassVar[token_type] = ...
    identifier: ClassVar[token_type] = ...
    keyword: ClassVar[token_type] = ...
    numeric_const: ClassVar[token_type] = ...
    operator: ClassVar[token_type] = ...
    string_const: ClassVar[token_type] = ...
    def __init__(self, value: int) -> None: ...
    def __eq__(self, other: object) -> bool: ...
    def __getstate__(self) -> int: ...
    def __hash__(self) -> int: ...
    def __index__(self) -> int: ...
    def __int__(self) -> int: ...
    def __ne__(self, other: object) -> bool: ...
    def __setstate__(self, state: int) -> None: ...
    @property
    def name(self) -> str: ...
    @property
    def value(self) -> int: ...

def aggregate(df: pandas.DataFrame, aggr_expr: str, group_expr: str = ..., connection: DuckDBPyConnection = ...) -> DuckDBPyRelation: ...
def alias(df: pandas.DataFrame, alias: str, connection: DuckDBPyConnection = ...) -> DuckDBPyRelation: ...
def arrow(arrow_object: object, connection: DuckDBPyConnection = ...) -> DuckDBPyRelation: ...
def connect(database: str = ..., read_only: bool = ..., config: object = ...) -> DuckDBPyConnection: ...
def df(df: pandas.DataFrame, connection: DuckDBPyConnection = ...) -> DuckDBPyRelation: ...
def distinct(df: pandas.DataFrame, connection: DuckDBPyConnection = ...) -> DuckDBPyRelation: ...
def filter(df: pandas.DataFrame, filter_expr: str, connection: DuckDBPyConnection = ...) -> DuckDBPyRelation: ...
def from_arrow(arrow_object: object, connection: DuckDBPyConnection = ...) -> DuckDBPyRelation: ...
def from_csv_auto(file_name: str, connection: DuckDBPyConnection = ...) -> DuckDBPyRelation: ...
def from_df(df: pandas.DataFrame, connection: DuckDBPyConnection = ...) -> DuckDBPyRelation: ...
@overload
def from_parquet(file_name: str, binary_as_string: bool, connection: DuckDBPyConnection = ...) -> DuckDBPyRelation: ...
@overload
def from_parquet(file_name: str, connection: DuckDBPyConnection = ...) -> DuckDBPyRelation: ...
def from_query(query: str, alias: str = ..., connection: DuckDBPyConnection = ...) -> DuckDBPyRelation: ...
def from_substrait(proto: bytes, connection: DuckDBPyConnection = ...) -> DuckDBPyRelation: ...
def get_substrait(query: str, connection: DuckDBPyConnection = ...) -> DuckDBPyRelation: ...
def get_substrait_json(query: str, connection: DuckDBPyConnection = ...) -> DuckDBPyRelation: ...
def limit(df: pandas.DataFrame, n: int, connection: DuckDBPyConnection = ...) -> DuckDBPyRelation: ...
def order(df: pandas.DataFrame, order_expr: str, connection: DuckDBPyConnection = ...) -> DuckDBPyRelation: ...
def project(df: pandas.DataFrame, project_expr: str, connection: DuckDBPyConnection = ...) -> DuckDBPyRelation: ...
def query(query: str, alias: str = ..., connection: DuckDBPyConnection = ...) -> DuckDBPyRelation: ...
def query_df(df: pandas.DataFrame, virtual_table_name: str, sql_query: str, connection: DuckDBPyConnection = ...) -> DuckDBPyResult: ...
def tokenize(query: str) -> object: ...
def values(values: object, connection: DuckDBPyConnection = ...) -> DuckDBPyRelation: ...
def write_csv(df: pandas.DataFrame, file_name: str, connection: DuckDBPyConnection = ...) -> None: ...
