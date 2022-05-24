import lru
import requests
import requests.adapters
from web3.utils.caching import (
    generate_cache_key,
)


def _remove_session(key, session):
    session.close()


_session_cache = lru.LRU(8, callback=_remove_session)


def _get_session(*args, **kwargs):
    cache_key = generate_cache_key((args, kwargs))
    if cache_key not in _session_cache:
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        _session_cache[cache_key] = session
    return _session_cache[cache_key]


def make_post_request(endpoint_uri, data, args: list = None, kwargs: dict = None):
    kwargs.setdefault('timeout', 10)
    session: requests.Session = _get_session(endpoint_uri)
    args = args or []
    response = session.post(endpoint_uri, data=data, *args, **kwargs)
    response.raise_for_status()
    return response.content
