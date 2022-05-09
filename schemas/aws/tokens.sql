CREATE EXTERNAL TABLE `ethereum.token`(
  `address` string,
  `symbol` string,
  `name` string,
  `decimals` bigint,
  `total_supply` DECIMAL(38,0),
  `block_number` bigint,
  `block_timestamp` bigint,
  `block_hash` string,
  `item_id` string,
  `item_timestamp` string)
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
WITH SERDEPROPERTIES ( 
  'paths'='address,block_hash,block_number,block_timestamp,decimals,item_id,item_timestamp,name,symbol,total_supply')
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://<your bucket>/tables/token/'
TBLPROPERTIES (
  'classification'='json', 
  'compressionType'='none'
)