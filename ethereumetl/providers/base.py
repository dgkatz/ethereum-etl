import requests
import requests.adapters
from web3.utils.request import _get_session


def make_post_request(endpoint_uri, data, args: list = None, kwargs: dict = None):
    kwargs.setdefault('timeout', 10)
    session: requests.Session = _get_session(endpoint_uri)
    adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    args = args or []
    response = session.post(endpoint_uri, data=data, *args, **kwargs)
    response.raise_for_status()
    return response.content
