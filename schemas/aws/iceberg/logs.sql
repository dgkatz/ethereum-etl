CREATE TABLE `ethereum_optimized.log`(
  `log_index` bigint,
  `transaction_hash` string, 
  `transaction_index` bigint, 
  `address` string, 
  `data` string, 
  `topics` array<string>, 
  `block_number` bigint, 
  `block_timestamp` bigint, 
  `block_hash` string, 
  `item_id` string, 
  `item_timestamp` string)
PARTITIONED BY (bucket(100, `transaction_hash`))
LOCATION
  's3://<your bucket>/optimized_tables/log/'
TBLPROPERTIES (
  'table_type'='iceberg'
)