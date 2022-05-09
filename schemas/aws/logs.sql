CREATE EXTERNAL TABLE `ethereum.log`(
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
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
WITH SERDEPROPERTIES ( 
  'paths'='address,block_hash,block_number,block_timestamp,data,item_id,item_timestamp,log_index,topics,transaction_hash,transaction_index')
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://<your bucket>/tables/log/'
TBLPROPERTIES (
  'classification'='json', 
  'compressionType'='none'
)