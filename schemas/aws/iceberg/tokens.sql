CREATE TABLE ethereum_optimized.token(
  address string,
  symbol string,
  name string,
  decimals bigint,
  total_supply string,
  block_number bigint,
  block_timestamp timestamp,
  block_hash string,
  item_id string,
  item_timestamp string)
PARTITIONED BY (bucket(100, address))
LOCATION
  's3://<your bucket>/optimized_tables/token/'
TBLPROPERTIES (
  'table_type'='iceberg'
)