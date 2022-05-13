insert into ethereum_optimized.token_transfer
select
	token_address,
	from_address,
	to_address,
	value,
	transaction_hash,
	log_index,
	block_number,
	from_unixtime(block_timestamp) as block_timestamp,
	block_hash,
	item_id,
	item_timestamp
from ethereum.token_transfer where block_number > (select coalesce(max(block_number), 0) from ethereum_optimized.token_transfer);

OPTIMIZE token_transfer REWRITE DATA USING BIN_PACK;