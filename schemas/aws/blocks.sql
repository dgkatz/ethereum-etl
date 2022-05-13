CREATE EXTERNAL TABLE `ethereum.block`(
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
  `difficulty` string,
  `total_difficulty` string,
  `size` bigint, 
  `extra_data` string, 
  `gas_limit` bigint, 
  `gas_used` bigint, 
  `timestamp` bigint, 
  `transaction_count` bigint, 
  `base_fee_per_gas` string, 
  `item_id` string, 
  `item_timestamp` string)
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
WITH SERDEPROPERTIES ( 
  'paths'='base_fee_per_gas,difficulty,extra_data,gas_limit,gas_used,hash,item_id,item_timestamp,logs_bloom,miner,nonce,number,parent_hash,receipts_root,sha3_uncles,size,state_root,timestamp,total_difficulty,transaction_count,transactions_root')
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://<your bucket>/tables/block'
TBLPROPERTIES (
  'classification'='json', 
  'compressionType'='none'
)