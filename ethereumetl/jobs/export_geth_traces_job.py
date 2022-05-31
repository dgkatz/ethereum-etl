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
import os
from threading import Lock

from blockchainetl.jobs.base_job import BaseJob
from ethereumetl.executors.batch_work_executor import BatchWorkExecutor
from ethereumetl.json_rpc_requests import generate_geth_trace_block_by_number_json_rpc
from ethereumetl.mainnet_daofork_state_changes import DAOFORK_BLOCK_NUMBER
from ethereumetl.mappers.geth_trace_mapper import EthGethTraceMapper
from ethereumetl.misc.retriable_value_error import RetriableValueError
from ethereumetl.utils import validate_range, rpc_response_to_result


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

        self.batch_work_executor = BatchWorkExecutor(1, max_workers, work_name='ExportGethTracesJob')
        self.item_exporter = item_exporter

        self.geth_trace_mapper = EthGethTraceMapper()
        self.cache_file = "geth_traces_cache.json"
        self.cache_file_lock = Lock()
        self.blocks_exported = set()
        self.blocks_exported_lock = Lock()

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        traces_from_cache = self._get_traces_from_cache()
        for block_trace in traces_from_cache:
            block_number = block_trace['block_number']
            self.blocks_exported.add(block_number)
            self.item_exporter.export_item(block_trace)

        blocks_to_export = list(range(self.start_block, self.end_block + 1))
        blocks_to_export = list(set(blocks_to_export).difference(self.blocks_exported))

        self.batch_work_executor.execute(
            blocks_to_export,
            self._export_batch,
            total_items=len(blocks_to_export)
        )

    def _get_traces_from_cache(self) -> list:
        try:
            with self.cache_file_lock, open(self.cache_file, "r") as cache_fp:
                geth_traces = json.load(cache_fp)
        except FileNotFoundError:
            return []
        return geth_traces

    def _export_batch(self, block_number_batch):
        # Remove untraceable blocks
        if 0 in block_number_batch:
            block_number_batch.remove(0)
        if DAOFORK_BLOCK_NUMBER in block_number_batch:
            block_number_batch.remove(DAOFORK_BLOCK_NUMBER)

        # Remove blocks already exported
        with self.blocks_exported_lock:
            block_number_batch = list(set(block_number_batch).difference(self.blocks_exported))

        if not block_number_batch:
            return

        trace_block_rpc = list(generate_geth_trace_block_by_number_json_rpc(block_number_batch))
        response = self.batch_web3_provider.make_batch_request(json.dumps(trace_block_rpc))

        failed_blocks = []
        for response_item in response:
            block_number = response_item.get('id')
            result = rpc_response_to_result(response_item)
            transaction_traces = []
            failed = False
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
                    failed = tx_trace['error']
                    break
                transaction_traces.append(tx_trace.get('result'))

            if failed:
                failed_blocks.append(block_number)
                logging.error(f"Response for block {block_number} contains a error: {failed}")
                continue

            geth_trace = self.geth_trace_mapper.json_dict_to_geth_trace({
                'block_number': block_number,
                'transaction_traces': transaction_traces,
            })
            self.item_exporter.export_item(self.geth_trace_mapper.geth_trace_to_dict(geth_trace))
            with self.blocks_exported_lock:
                self.blocks_exported.add(block_number)

        if failed_blocks:
            raise RetriableValueError(
                f"Failed to export traces for {len(failed_blocks)} blocks: {', '.join(map(str, failed_blocks))}")

    def _end(self):
        try:
            self.batch_work_executor.shutdown()
        except Exception as exc:
            with self.cache_file_lock, open(self.cache_file, "w") as cache_fp:
                cache_fp.write(json.dumps(self.item_exporter.get_items('trace')))
            raise exc
        else:
            try:
                os.remove(self.cache_file)
            except FileNotFoundError:
                pass
        finally:
            self.item_exporter.close()
