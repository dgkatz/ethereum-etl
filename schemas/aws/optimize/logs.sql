insert into ethereum_optimized.log
select
	log_index,
	transaction_hash,
	transaction_index,
	address,
	data,
	topics,
	block_number,
	from_unixtime(block_timestamp) as block_timestamp,
	block_hash,
	item_id,
	item_timestamp
from ethereum.log where block_number > (select coalesce(max(block_number), 0) from ethereum_optimized.log);

OPTIMIZE log REWRITE DATA USING BIN_PACK;