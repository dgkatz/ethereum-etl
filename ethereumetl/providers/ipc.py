# The MIT License (MIT)
#
# Copyright (c) 2016 Piper Merriam
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
import socket
from concurrent.futures import ThreadPoolExecutor

from web3.providers.ipc import IPCProvider
from web3.utils.threads import (
    Timeout,
)

try:
    from json import JSONDecodeError
except ImportError:
    JSONDecodeError = ValueError


# Mostly copied from web3.py/providers/ipc.py. Supports batch requests.
# Will be removed once batch feature is added to web3.py https://github.com/ethereum/web3.py/issues/832
# Also see this optimization https://github.com/ethereum/web3.py/pull/849
class BatchIPCProvider(IPCProvider):
    _socket = None

    @property
    def batching_supported(self):
        return True

    def mod_make_request(self, text):
        request = text.encode('utf-8')
        with self._lock, self._socket as sock:
            try:
                sock.sendall(request)
            except BrokenPipeError:
                # one extra attempt, then give up
                sock = self._socket.reset()
                sock.sendall(request)

            sock.settimeout(10)

            raw_response = b""
            with Timeout(self.timeout) as timeout:
                while True:
                    try:
                        raw_response += sock.recv(4096)
                    except socket.timeout:
                        timeout.sleep(0)
                        continue
                    if raw_response == b"":
                        timeout.sleep(0)
                    elif has_valid_json_rpc_ending(raw_response):
                        try:
                            response = json.loads(raw_response.decode('utf-8'))
                        except JSONDecodeError:
                            timeout.sleep(0)
                            continue
                        except RecursionError:
                            logging.error(f"Failed to parse due to RecursionError: {raw_response.decode('utf-8')}")
                            timeout.sleep(0)
                            continue
                        else:
                            return response
                    else:
                        timeout.sleep(0)
                        continue

    def make_batch_request(self, text):
        if not self.batching_supported:
            responses = []
            with ThreadPoolExecutor() as pool:
                for response in pool.map(self.mod_make_request,
                                         (json.dumps(rpc) for rpc in json.loads(text))):
                    responses.append(response)
            return responses
        return self.mod_make_request(text=text)


# A valid JSON RPC response can only end in } or ] http://www.jsonrpc.org/specification
def has_valid_json_rpc_ending(raw_response):
    for valid_ending in [b"}\n", b"]\n"]:
        if raw_response.endswith(valid_ending):
            return True
    else:
        return False
