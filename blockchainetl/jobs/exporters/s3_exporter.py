from collections import defaultdict
from typing import Iterator

import pandas as pd
import pyarrow as pa


class S3ItemExporter:
    def __init__(self, bucket: str, prefix: str):
        self.bucket = bucket
        self.prefix = normalize_path(prefix)

    def open(self):
        pass

    def export_items(self, items, start_block: int, end_block: int):
        file_suffix = f'{start_block}_{end_block}.parquet'
        items = group_items_by_type(items=items)
        for item_type, items_ in items.items():
            df = pd.DataFrame(items_)
            destination_blob_name = f's3://{self.bucket}/{self.prefix}/{item_type}s/{file_suffix}'
            pa_schema = item_type_to_pyarrow_schema(item_type=item_type)
            for col_name in pa_schema.names:
                if pa_schema.field(col_name).type == 'string':
                    df[col_name] = df[col_name].astype(str)
            df.to_parquet(destination_blob_name, engine="pyarrow", index=False, schema=pa_schema)

    def close(self):
        pass


def group_items_by_type(items: list):
    grouped_items = defaultdict(list)
    for item in items:
        item_type = item.pop('type')
        grouped_items[item_type].append(item)
    return grouped_items


def normalize_path(p):
    if p is None:
        p = ''
    p = p.lstrip('/')
    p = p.rstrip("/")
    return p


def item_type_to_pyarrow_schema(item_type: str):
    if item_type == "block":
        return athena_schema_to_pyarrow(athena_schema={'number': 'bigint', 'hash': 'string', 'parent_hash': 'string', 'nonce': 'string', 'sha3_uncles': 'string', 'logs_bloom': 'string', 'transactions_root': 'string', 'state_root': 'string', 'receipts_root': 'string', 'miner': 'string', 'difficulty': 'string', 'total_difficulty': 'string', 'size': 'bigint', 'extra_data': 'string', 'gas_limit': 'bigint', 'gas_used': 'bigint', 'timestamp': 'bigint', 'transaction_count': 'bigint', 'base_fee_per_gas': 'string', 'item_id': 'string', 'item_timestamp': 'string'})
    elif item_type == "transaction":
        return athena_schema_to_pyarrow(athena_schema={'hash': 'string', 'nonce': 'bigint', 'transaction_index': 'bigint', 'from_address': 'string', 'to_address': 'string', 'value': 'string', 'gas': 'bigint', 'gas_price': 'bigint', 'input': 'string', 'block_timestamp': 'bigint', 'block_number': 'bigint', 'block_hash': 'string', 'max_fee_per_gas': 'bigint', 'max_priority_fee_per_gas': 'bigint', 'transaction_type': 'int', 'receipt_cumulative_gas_used': 'bigint', 'receipt_gas_used': 'bigint', 'receipt_contract_address': 'string', 'receipt_root': 'string', 'receipt_status': 'int', 'receipt_effective_gas_price': 'bigint', 'item_id': 'string', 'item_timestamp': 'string'})
    elif item_type == "log":
        return athena_schema_to_pyarrow(athena_schema={'log_index': 'bigint', 'transaction_hash': 'string', 'transaction_index': 'bigint', 'address': 'string', 'data': 'string', 'topics': 'array<string>', 'block_number': 'bigint', 'block_timestamp': 'bigint', 'block_hash': 'string', 'item_id': 'string', 'item_timestamp': 'string'})
    elif item_type == "trace":
        return athena_schema_to_pyarrow(athena_schema={'transaction_index': 'bigint', 'from_address': 'string', 'to_address': 'string', 'value': 'string', 'input': 'string', 'output': 'string', 'trace_type': 'string', 'call_type': 'string', 'reward_type': 'string', 'gas': 'bigint', 'gas_used': 'bigint', 'subtraces': 'bigint', 'trace_address': 'array<int>', 'error': 'string', 'status': 'int', 'transaction_hash': 'string', 'block_number': 'bigint', 'trace_id': 'string', 'trace_index': 'string', 'block_timestamp': 'bigint', 'block_hash': 'string', 'item_id': 'string', 'item_timestamp': 'string'})
    elif item_type == "contract":
        return athena_schema_to_pyarrow(athena_schema={'address': 'string', 'bytecode': 'string', 'function_sighashes': 'array<string>', 'is_erc20': 'boolean', 'is_erc721': 'boolean', 'block_number': 'bigint', 'block_timestamp': 'bigint', 'block_hash': 'string', 'item_id': 'string', 'item_timestamp': 'string'})
    elif item_type == "token":
        return athena_schema_to_pyarrow(athena_schema={'address': 'string', 'symbol': 'string', 'name': 'string', 'decimals': 'bigint', 'total_supply': 'string', 'block_number': 'bigint', 'block_timestamp': 'bigint', 'block_hash': 'string', 'item_id': 'string', 'item_timestamp': 'string'})
    elif item_type == "token_transfer":
        return athena_schema_to_pyarrow(athena_schema={'token_address': 'string', 'from_address': 'string', 'to_address': 'string', 'value': 'string', 'transaction_hash': 'string', 'log_index': 'bigint', 'block_number': 'bigint', 'block_timestamp': 'bigint', 'block_hash': 'string', 'item_id': 'string', 'item_timestamp': 'string'})


def athena_schema_to_pyarrow(athena_schema: dict[str, str]) -> pa.Schema:
    schema = []
    for column, athena_type in athena_schema.items():
        schema.append(pa.field(name=column.lower(), type=_athena_dtype_to_pyarrow(athena_type)))
    pa_schema = pa.schema(schema)
    return pa_schema


def _athena_dtype_to_pyarrow(dtype: str) -> pa.DataType:
    """Athena to PyArrow data types conversion."""
    dtype = dtype.lower().replace(" ", "")
    if dtype == "tinyint":
        return pa.int8()
    if dtype == "smallint":
        return pa.int16()
    if dtype in ("int", "integer"):
        return pa.int32()
    if dtype == "bigint":
        return pa.int64()
    if dtype in ("float", "real"):
        return pa.float32()
    if dtype == "double":
        return pa.float64()
    if dtype == "boolean":
        return pa.bool_()
    if (dtype == "string") or dtype.startswith("char") or dtype.startswith("varchar"):
        return pa.string()
    if dtype == "timestamp":
        return pa.timestamp(unit="ns")
    if dtype == "date":
        return pa.date32()
    if dtype in ("binary" or "varbinary"):
        return pa.binary()
    if dtype.startswith("decimal") is True:
        try:
            precision, scale = dtype.replace("decimal(", "").replace(")", "").split(sep=",")
        except ValueError:
            return pa.int64()
        return pa.decimal128(int(precision), int(scale))
    if dtype.startswith("array") is True:
        return pa.list_(_athena_dtype_to_pyarrow(dtype=dtype[6:-1]), -1)
    if dtype.startswith("struct") is True:
        return pa.struct(
            [(f.split(":", 1)[0], _athena_dtype_to_pyarrow(f.split(":", 1)[1])) for f in _split_struct(dtype[7:-1])])
    if dtype.startswith("map") is True:
        parts: list[str] = _split_map(s=dtype[4:-1])
        return pa.map_(_athena_dtype_to_pyarrow(parts[0]), _athena_dtype_to_pyarrow(parts[1]))
    if dtype.startswith("set") is True:
        return pa.string()
    raise Exception(f"Unsupported Athena type: {dtype}")


def _split_map(s: str) -> list[str]:
    parts: list[str] = list(_split_fields(s=s))
    if len(parts) != 2:
        raise RuntimeError(f"Invalid map fields: {s}")
    return parts


def _split_fields(s: str) -> Iterator[str]:
    counter: int = 0
    last: int = 0
    for i, x in enumerate(s):
        if x == "<":
            counter += 1
        elif x == ">":
            counter -= 1
        elif x == "," and counter == 0:
            yield s[last:i]
            last = i + 1
    yield s[last:]
