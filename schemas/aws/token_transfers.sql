CREATE EXTERNAL TABLE `ethereum.token_transfer`(
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
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
WITH SERDEPROPERTIES ( 
  'paths'='block_hash,block_number,block_timestamp,from_address,item_id,item_timestamp,log_index,to_address,token_address,transaction_hash,value')
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://<your bucket>/tables/token_transfer/'
TBLPROPERTIES (
  'classification'='json', 
  'compressionType'='none'
)