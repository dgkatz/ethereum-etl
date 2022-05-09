CREATE TABLE ethereum_optimized.transaction(
  hash string,
  nonce bigint,
  transaction_index bigint,
  from_address string,
  to_address string,
  value decimal(38,0),
  gas bigint,
  gas_price bigint,
  input string,
  block_timestamp timestamp,
  block_number bigint,
  block_hash string,
  max_fee_per_gas bigint,
  max_priority_fee_per_gas bigint,
  transaction_type int,
  receipt_cumulative_gas_used bigint,
  receipt_gas_used bigint,
  receipt_contract_address string,
  receipt_root string,
  receipt_status int,
  receipt_effective_gas_price bigint,
  item_id string,
  item_timestamp string)
PARTITIONED BY (bucket(10, block_number), bucket(10, hash))
LOCATION
  's3://<your bucket>/optimized_tables/transaction/'
TBLPROPERTIES (
  'table_type'='iceberg'
)