import boto3
from requests_auth_aws_sigv4 import AWSSigV4
from web3.utils.request import _get_session

from ethereumetl.providers.base import make_post_request
from ethereumetl.providers.rpc import BatchHTTPProvider


class AWSHTTPProvider(BatchHTTPProvider):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.aws_auth = AWSSigV4(
            "managedblockchain",
            session=boto3.Session()
        )

    @property
    def batching_supported(self):
        return False

    def make_request(self, method, params):
        self.logger.debug("Making request HTTP. URI: %s, Method: %s",
                          self.endpoint_uri, method)
        request_data = self.encode_rpc_request(method, params).decode()
        session = _get_session(self.endpoint_uri)
        response = session.post(
            self.endpoint_uri,
            data=request_data,
            auth=self.aws_auth,
            **self.get_request_kwargs()
        )
        response.raise_for_status()
        raw_response = response.content
        response = self.decode_rpc_response(raw_response)
        self.logger.debug("Getting response HTTP. URI: %s, "
                          "Method: %s, Response: %s",
                          self.endpoint_uri, method, response)
        return response

    def mod_make_request(self, text: str):
        self.logger.debug("Making request HTTP. URI: %s, Request: %s",
                          self.endpoint_uri, text)
        request_data = text.encode('utf-8')
        request_kwargs = self.get_request_kwargs()
        request_kwargs.update({"auth": self.aws_auth})
        raw_response = make_post_request(
            endpoint_uri=self.endpoint_uri,
            data=request_data,
            kwargs=request_kwargs
        )
        response = self.decode_rpc_response(raw_response)
        self.logger.debug("Getting response HTTP. URI: %s, "
                          "Request: %s, Response: %s",
                          self.endpoint_uri, text, response)
        return response
