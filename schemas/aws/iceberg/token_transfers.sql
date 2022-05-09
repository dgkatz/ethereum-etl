CREATE TABLE `ethereum_optimized.token_transfer`(
  `token_address` string, 
  `from_address` string, 
  `to_address` string, 
  `value` decimal(38,0),
  `transaction_hash` string, 
  `log_index` bigint, 
  `block_number` bigint, 
  `block_timestamp` bigint, 
  `block_hash` string, 
  `item_id` string, 
  `item_timestamp` string)
PARTITIONED BY (bucket(100, `token_address`))
LOCATION
  's3://<your bucket>/optimized_tables/token_transfer/'
TBLPROPERTIES (
  'table_type'='iceberg'
)