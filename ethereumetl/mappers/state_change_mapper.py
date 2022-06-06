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
import hashlib

from ethereumetl.domain.state_change import EthAccountStateChange


class EthAccountStateChangeMapper(object):
    def json_dict_to_account_state_change(self, json_dict):
        tx_hash = json_dict['transactionHash']
        account_state_changes = []
        for account_address, state_changes in json_dict.get('stateDiff', {}).items():
            account_state_change = EthAccountStateChange()
            account_state_change.account_address = account_address
            account_state_change.transaction_hash = tx_hash

            # Parse account balance change
            balance_state_change = state_changes.get('balance', {})
            if '*' in balance_state_change:
                account_state_change.balance_before = int(balance_state_change['*']['from'], 16)
                account_state_change.balance_after = int(balance_state_change['*']['to'], 16)
                account_state_change.balance_change = account_state_change.balance_after - account_state_change.balance_before
            elif '+' in balance_state_change:
                account_state_change.balance_before = 0
                account_state_change.balance_after = int(balance_state_change['+'], 16)
                account_state_change.balance_change = account_state_change.balance_after

            # Parse account code change
            code_state_change = state_changes.get('code', {})
            if '+' in code_state_change and code_state_change['+'] != '0x':
                account_state_change.code_after = code_state_change['+']

            # Parse account nonce change
            nonce_state_change = state_changes.get('nonce', {})
            if '*' in nonce_state_change:
                account_state_change.nonce_before = int(nonce_state_change['*']['from'], 16)
                account_state_change.nonce_after = int(nonce_state_change['*']['to'], 16)
            elif '+' in nonce_state_change:
                account_state_change.nonce_before = 0
                account_state_change.nonce_after = int(nonce_state_change['+'], 16)

            # Parse account storage change
            raw_storage_state_changes = state_changes.get('storage', {})
            account_storage_before = []
            account_storage_after = []
            for storage_address, storage_state_change in raw_storage_state_changes.items():
                if '*' in storage_state_change:
                    storage_before = storage_state_change['*']['from']
                    storage_after = storage_state_change['*']['to']
                elif '+' in storage_state_change:
                    storage_before = None
                    storage_after = storage_state_change['+']
                else:
                    continue
                account_storage_before.append({'key': account_address, 'value': storage_before})
                account_storage_after.append({'key': account_address, 'value': storage_after})
            account_state_change.storage_before = account_storage_before or None
            account_state_change.storage_after = account_storage_after or None

            account_state_changes.append(account_state_change)

        return account_state_changes

    def account_state_change_to_dict(self, account_state_change: EthAccountStateChange):
        return {
            'type': 'account_state_change',
            'block_number': account_state_change.block_number,
            'transaction_hash': account_state_change.transaction_hash,
            'account_address': account_state_change.account_address,
            'balance_before': account_state_change.balance_before,
            'balance_after': account_state_change.balance_after,
            'balance_change': account_state_change.balance_change,
            'code_before': account_state_change.code_before,
            'code_after': account_state_change.code_after,
            'nonce_before': account_state_change.nonce_before,
            'nonce_after': account_state_change.nonce_after,
            'storage_before': account_state_change.storage_before,
            'storage_after': account_state_change.storage_after,
        }


def hash_trace_props(block_numer: int, tx_index: int, trace_address: list) -> str:
    return hashlib.sha256(f"{block_numer}{tx_index}{str(trace_address)}".encode("utf-8")).hexdigest()
