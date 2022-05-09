insert into ethereum_optimized.block
select number,
	hash,
	parent_hash,
	nonce,
	sha3_uncles,
	logs_bloom,
	transactions_root,
	state_root,
	receipts_root,
	miner,
	difficulty,
	total_difficulty,
	size,
	extra_data,
	gas_limit,
	gas_used,
	from_unixtime(timestamp) as created_timestamp,
	transaction_count,
	base_fee_per_gas,
	item_id,
	item_timestamp
from ethereum.block where number > (select coalesce(max(number), 0) from ethereum_optimized.block);

OPTIMIZE block REWRITE DATA USING BIN_PACK;