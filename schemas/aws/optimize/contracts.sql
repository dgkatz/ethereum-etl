insert into ethereum_optimized.contract
select
	address,
	bytecode,
	function_sighashes,
	is_erc20,
	is_erc721,
	block_number,
	from_unixtime(block_timestamp) as block_timestamp,
	block_hash,
	item_id,
	item_timestamp
from ethereum.contract where block_number > (select coalesce(max(block_number), 0) from ethereum_optimized.contract);

OPTIMIZE contract REWRITE DATA USING BIN_PACK;