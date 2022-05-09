CREATE EXTERNAL TABLE `ethereum.contract`(
  `address` string,
  `bytecode` string, 
  `function_sighashes` array<string>, 
  `is_erc20` boolean, 
  `is_erc721` boolean, 
  `block_number` bigint, 
  `block_timestamp` bigint, 
  `block_hash` string, 
  `item_id` string, 
  `item_timestamp` string)
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
WITH SERDEPROPERTIES ( 
  'paths'='address,block_hash,block_number,block_timestamp,bytecode,function_sighashes,is_erc20,is_erc721,item_id,item_timestamp')
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://<your bucket>/tables/contract/'
TBLPROPERTIES (
  'classification'='json', 
  'compressionType'='none'
)