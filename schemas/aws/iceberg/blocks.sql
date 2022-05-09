CREATE TABLE `ethereum_optimized.block`(
  `number` bigint,
  `hash` string, 
  `parent_hash` string, 
  `nonce` string, 
  `sha3_uncles` string, 
  `logs_bloom` string, 
  `transactions_root` string, 
  `state_root` string, 
  `receipts_root` string, 
  `miner` string, 
  `difficulty` decimal(38,0),
  `total_difficulty` decimal(38,0),
  `size` bigint, 
  `extra_data` string, 
  `gas_limit` bigint, 
  `gas_used` bigint, 
  `timestamp` timestamp,
  `transaction_count` bigint, 
  `base_fee_per_gas` string, 
  `item_id` string, 
  `item_timestamp` string)
PARTITIONED BY (bucket(100, `number`))
LOCATION
  's3://<your bucket>/optimized_tables/block'
TBLPROPERTIES (
  'table_type'='iceberg'
)