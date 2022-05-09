CREATE EXTERNAL TABLE `trace`(
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
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
WITH SERDEPROPERTIES ( 
  'paths'='block_hash,block_number,block_timestamp,call_type,error,from_address,gas,gas_used,input,item_id,item_timestamp,output,reward_type,status,subtraces,to_address,trace_address,trace_id,trace_index,trace_type,transaction_hash,transaction_index,value')
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://<your bucket>/tables/trace/'
TBLPROPERTIES (
  'classification'='json', 
  'compressionType'='none'
)