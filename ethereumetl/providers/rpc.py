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
from concurrent.futures import ThreadPoolExecutor

from web3 import HTTPProvider

from ethereumetl.providers.base import make_post_request


# Mostly copied from web3.py/providers/rpc.py. Supports batch requests.
# Will be removed once batch feature is added to web3.py https://github.com/ethereum/web3.py/issues/832
class BatchHTTPProvider(HTTPProvider):

    @property
    def batching_supported(self):
        return True

    def mod_make_request(self, text: str):
        self.logger.debug("Making request HTTP. URI: %s, Request: %s",
                          self.endpoint_uri, text)
        request_data = text.encode('utf-8')
        raw_response = make_post_request(
            endpoint_uri=self.endpoint_uri,
            data=request_data,
            kwargs=self.get_request_kwargs()
        )
        response = self.decode_rpc_response(raw_response)
        self.logger.debug("Getting response HTTP. URI: %s, "
                          "Request: %s, Response: %s",
                          self.endpoint_uri, text, response)
        return response

    def make_batch_request(self, text):
        if not self.batching_supported:
            responses = []
            with ThreadPoolExecutor() as pool:
                for response in pool.map(self.mod_make_request,
                                         (json.dumps(rpc) for rpc in json.loads(text))):
                    responses.append(response)
            return responses
        self.logger.debug("Making request HTTP. URI: %s, Request: %s",
                          self.endpoint_uri, text)
        return self.mod_make_request(text=text)
