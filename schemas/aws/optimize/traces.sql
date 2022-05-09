insert into ethereum_optimized.trace
select
	transaction_index,
	from_address,
	to_address,
	cast(value as decimal),
	input,
	output,
	trace_type,
	call_type,
	reward_type,
	gas,
	gas_used,
	subtraces,
	trace_address,
	error,
	status,
	transaction_hash,
	block_number,
	trace_id,
	trace_index,
	from_unixtime(block_timestamp) as block_timestamp,
	block_hash,
	item_id,
	item_timestamp
from ethereum.trace where block_number > (select coalesce(max(block_number), 0) from ethereum_optimized.trace)

OPTIMIZE trace REWRITE DATA USING BIN_PACK