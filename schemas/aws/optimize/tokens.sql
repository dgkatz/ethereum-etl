insert into ethereum_optimized.token
select
	address,
	symbol,
	name,
	decimals,
	total_supply,
	block_number,
	from_unixtime(block_timestamp) as block_timestamp,
	block_hash,
	item_id,
	item_timestamp
from ethereum.token where block_number > (select coalesce(max(block_number), 0) from ethereum_optimized.token);

OPTIMIZE token REWRITE DATA USING BIN_PACK;