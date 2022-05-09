CREATE EXTERNAL TABLE `ethereum.transaction`(
  `hash` string,
  `nonce` bigint,
  `transaction_index` bigint,
  `from_address` string,
  `to_address` string,
  `value` decimal(38,0),
  `gas` bigint,
  `gas_price` bigint,
  `input` string,
  `block_timestamp` bigint,
  `block_number` bigint,
  `block_hash` string,
  `max_fee_per_gas` bigint,
  `max_priority_fee_per_gas` bigint,
  `transaction_type` int,
  `receipt_cumulative_gas_used` bigint,
  `receipt_gas_used` bigint,
  `receipt_contract_address` string,
  `receipt_root` string,
  `receipt_status` int,
  `receipt_effective_gas_price` bigint,
  `item_id` string,
  `item_timestamp` string)
ROW FORMAT SERDE
  'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'paths'='block_hash,block_number,block_timestamp,from_address,gas,gas_price,hash,input,item_id,item_timestamp,max_fee_per_gas,max_priority_fee_per_gas,nonce,receipt_contract_address,receipt_cumulative_gas_used,receipt_effective_gas_price,receipt_gas_used,receipt_root,receipt_status,to_address,transaction_index,transaction_type,value')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://<your bucket>/tables/transaction/'
TBLPROPERTIES (
  'classification'='json',
  'compressionType'='none'
)