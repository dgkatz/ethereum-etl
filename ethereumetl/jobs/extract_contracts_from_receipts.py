# MIT License
#
# Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com
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

from blockchainetl.jobs.base_job import BaseJob
from ethereumetl.executors.batch_work_executor import BatchWorkExecutor
from ethereumetl.json_rpc_requests import generate_get_code_json_rpc
from ethereumetl.mappers.contract_mapper import EthContractMapper
from ethereumetl.service.eth_contract_service import EthContractService
from ethereumetl.utils import rpc_response_to_result


# Extract contracts
class ExtractContractsFromReceiptsJob(BaseJob):
    def __init__(
            self,
            receipts_iterable,
            batch_size,
            batch_web3_provider,
            max_workers,
            item_exporter):
        self.batch_web3_provider = batch_web3_provider
        self.receipts_iterable = receipts_iterable

        self.batch_work_executor = BatchWorkExecutor(batch_size, max_workers, work_name='ExtractContractsFromReceiptsJob')
        self.item_exporter = item_exporter

        self.contract_service = EthContractService()
        self.contract_mapper = EthContractMapper()

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        self.batch_work_executor.execute(self.receipts_iterable, self._extract_contracts)

    def _extract_contracts(self, receipts):
        contract_addresses_and_block_numbers = [(receipt['contract_address'], receipt['block_number'])
                                                for receipt in receipts
                                                if receipt['status'] == 1 and receipt['contract_address']]

        if not contract_addresses_and_block_numbers:
            return
        contract_addresses, block_numbers = zip(*contract_addresses_and_block_numbers)
        contracts_code_rpc = list(generate_get_code_json_rpc(contract_addresses))
        response_batch = self.batch_web3_provider.make_batch_request(json.dumps(contracts_code_rpc))

        for response in response_batch:
            # request id is the index of the contract address in contract_addresses list
            request_id = response['id']
            result = rpc_response_to_result(response)
            contract_address = contract_addresses[request_id]
            block_number = block_numbers[request_id]
            contract = self.contract_mapper.rpc_result_to_contract(contract_address, result)
            bytecode = contract.bytecode
            function_sighashes = self.contract_service.get_function_sighashes(bytecode)
            contract.function_sighashes = function_sighashes
            contract.is_erc20 = self.contract_service.is_erc20_contract(function_sighashes)
            contract.is_erc721 = self.contract_service.is_erc721_contract(function_sighashes)
            contract.block_number = block_number

            self.item_exporter.export_item(self.contract_mapper.contract_to_dict(contract))

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()
