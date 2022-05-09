CREATE TABLE `ethereum_optimized.trace`(
  `transaction_index` bigint,
  `from_address` string, 
  `to_address` string, 
  `value` decimal(38,0),
  `input` string,
  `output` string, 
  `trace_type` string, 
  `call_type` string, 
  `reward_type` string, 
  `gas` bigint,
  `gas_used` bigint,
  `subtraces` bigint,
  `trace_address` array<int>, 
  `error` string, 
  `status` int, 
  `transaction_hash` string, 
  `block_number` bigint,
  `trace_id` string, 
  `trace_index` string, 
  `block_timestamp` bigint,
  `block_hash` string, 
  `item_id` string, 
  `item_timestamp` string)
PARTITIONED BY (bucket(100, `block_number`))
LOCATION
  's3://<your bucket>/optimized_tables/trace/'
TBLPROPERTIES (
  'table_type'='iceberg'
)