CREATE TABLE `ethereum_optimized.contract`(
  `address` string,
  `bytecode` string, 
  `function_sighashes` array<string>, 
  `is_erc20` boolean, 
  `is_erc721` boolean, 
  `block_number` bigint, 
  `block_timestamp` timestamp,
  `block_hash` string, 
  `item_id` string, 
  `item_timestamp` string)
PARTITIONED BY (bucket(20, `address`), is_erc20, is_erc721)
LOCATION
  's3://<your bucket>/optimized_tables/contract/'
TBLPROPERTIES (
  'table_type'='iceberg'
)