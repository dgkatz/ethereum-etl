# MIT License
#
# Copyright (c) 2018 Evgeniy Filatov, evgeniyfilatov@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import json
import logging

from ethereumetl.misc.retriable_value_error import RetriableValueError
from ethereumetl.executors.batch_work_executor import BatchWorkExecutor
from ethereumetl.json_rpc_requests import generate_trace_block_by_number_json_rpc
from blockchainetl.jobs.base_job import BaseJob
from ethereumetl.mappers.geth_trace_mapper import EthGethTraceMapper
from ethereumetl.utils import validate_range, rpc_response_to_result
from ethereumetl.mainnet_daofork_state_changes import DAOFORK_BLOCK_NUMBER

# Exports geth traces
class ExportGethTracesJob(BaseJob):
    def __init__(
            self,
            start_block,
            end_block,
            batch_size,
            batch_web3_provider,
            max_workers,
            item_exporter):
        validate_range(start_block, end_block)
        self.start_block = start_block
        self.end_block = end_block

        self.batch_web3_provider = batch_web3_provider

        self.batch_work_executor = BatchWorkExecutor(batch_size, max_workers)
        self.item_exporter = item_exporter

        self.geth_trace_mapper = EthGethTraceMapper()

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        self.batch_work_executor.execute(
            range(self.start_block, self.end_block + 1),
            self._export_batch,
            total_items=self.end_block - self.start_block + 1
        )

    def _export_batch(self, block_number_batch):
        # Remove untraceable blocks
        if 0 in block_number_batch:
            block_number_batch.remove(0)
        if DAOFORK_BLOCK_NUMBER in block_number_batch:
            block_number_batch.remove(DAOFORK_BLOCK_NUMBER)

        trace_block_rpc = list(generate_trace_block_by_number_json_rpc(block_number_batch))
        response = self.batch_web3_provider.make_batch_request(json.dumps(trace_block_rpc))

        for response_item in response:
            block_number = response_item.get('id')
            result = rpc_response_to_result(response_item)
            transaction_traces = []
            for tx_trace in result:
                if 'error' in tx_trace:
                    # Handle responses like:
                    # [{
                    #     error: "execution timeout"
                    # }, {
                    #     result: {
                    #       from: "0x3143e1cabc3547acde8b630e0dea3b1dfebb3cb0",
                    #       gas: "0x10d88",
                    #       gasUsed: "0x0",
                    #       input: "0x",
                    #       output: "0x",
                    #       to: "0xf8165fe1c2cc5360049e2b9c6bd88432a01c0d24",
                    #       type: "CALL",
                    #       value: "0xb1a2bc2ec50000"
                    #     }
                    # }]
                    logging.error(f"Response contains a error: {tx_trace['error']}")
                    raise RetriableValueError(tx_trace['error'])
                transaction_traces.append(tx_trace.get('result'))

            geth_trace = self.geth_trace_mapper.json_dict_to_geth_trace({
                'block_number': block_number,
                'transaction_traces': transaction_traces,
            })

            self.item_exporter.export_item(self.geth_trace_mapper.geth_trace_to_dict(geth_trace))

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()
