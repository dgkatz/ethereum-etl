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

from ethereumetl.mainnet_daofork_state_changes import DAOFORK_BLOCK_NUMBER
from blockchainetl.jobs.base_job import BaseJob
from ethereumetl.executors.batch_work_executor import BatchWorkExecutor
from ethereumetl.json_rpc_requests import generate_trace_state_diff_block_by_number_json_rpc
from ethereumetl.mappers.state_change_mapper import EthAccountStateChangeMapper
from ethereumetl.utils import validate_range, rpc_response_to_result


class ExportStateChangesJob(BaseJob):
    def __init__(
            self,
            start_block,
            end_block,
            batch_size,
            batch_web3_provider,
            item_exporter,
            max_workers):
        validate_range(start_block, end_block)
        self.start_block = start_block
        self.end_block = end_block

        self.batch_web3_provider = batch_web3_provider

        # TODO: use batch_size when this issue is fixed https://github.com/paritytech/parity-ethereum/issues/9822
        self.batch_work_executor = BatchWorkExecutor(batch_size, max_workers, work_name='ExportStateChangesJob')
        self.item_exporter = item_exporter

        self.account_state_change_mapper = EthAccountStateChangeMapper()

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        self.batch_work_executor.execute(
            range(self.start_block, self.end_block + 1),
            self._export_batch,
            total_items=self.end_block - self.start_block + 1
        )

    def _export_batch(self, block_number_batch):
        try:
            block_number_batch.remove(0)
            block_number_batch.remove(DAOFORK_BLOCK_NUMBER)
        except ValueError:
            pass

        all_account_state_changes = []

        trace_block_rpc = list(generate_trace_state_diff_block_by_number_json_rpc(block_number_batch))
        response = self.batch_web3_provider.make_batch_request(json.dumps(trace_block_rpc))

        if response is None:
            raise ValueError('Response from the node is None. Is the node fully synced? Is the node started with tracing enabled? Is trace_block API enabled?')

        for response_item in response:
            block_number = response_item['id']
            json_traces = rpc_response_to_result(response_item)
            for json_trace in json_traces:
                account_state_changes = self.account_state_change_mapper.json_dict_to_account_state_change(json_trace)
                for account_state_change in account_state_changes:
                    account_state_change.block_number = block_number
                all_account_state_changes.extend(account_state_changes)

        for account_state_changes in all_account_state_changes:
            self.item_exporter.export_item(
                self.account_state_change_mapper.account_state_change_to_dict(account_state_changes)
            )

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()


def calculate_trace_indexes(traces):
    # Only works if traces were originally ordered correctly which is the case for Parity traces
    for ind, trace in enumerate(traces):
        trace.trace_index = ind


if __name__ == '__main__':
    from ethereumetl.providers.auto import get_provider_from_uri
    from ethereumetl.thread_local_proxy import ThreadLocalProxy
    from blockchainetl.jobs.exporters.in_memory_item_exporter import InMemoryItemExporter

    exporter = InMemoryItemExporter(item_types=['account_state_change'])
    job = ExportStateChangesJob(
        start_block=14849603,
        end_block=14849603,
        batch_size=10,
        batch_web3_provider=ThreadLocalProxy(lambda: get_provider_from_uri("http://localhost:8545", timeout=50, batch=True)),
        item_exporter=exporter,
        max_workers=1
    )
    job.run()
    print(exporter)
