insert into ethereum_optimized.transaction
select
	hash,
	nonce,
	transaction_index,
	from_address,
	to_address,
	cast(value as decimal) as value,
	gas,
	gas_price,
	input,
	from_unixtime(block_timestamp) as block_timestamp,
	block_number,
	block_hash,
	max_fee_per_gas,
	max_priority_fee_per_gas,
	transaction_type,
	receipt_cumulative_gas_used,
	receipt_gas_used,
	receipt_contract_address,
	receipt_root,
	receipt_status,
	receipt_effective_gas_price,
	item_id,
	item_timestamp
from ethereum.transaction where block_number > (select coalesce(max(block_number), 0) from ethereum_optimized.transaction);

OPTIMIZE transaction REWRITE DATA USING BIN_PACK;