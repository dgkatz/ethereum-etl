from web3.utils.request import _get_session


def make_post_request(endpoint_uri, data, args: list = None, kwargs: dict = None):
    kwargs.setdefault('timeout', 10)
    session = _get_session(endpoint_uri)
    args = args or []
    response = session.post(endpoint_uri, data=data, *args, **kwargs)
    response.raise_for_status()
    return response.content
