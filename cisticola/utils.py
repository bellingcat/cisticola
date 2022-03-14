import requests
from loguru import logger
import time

def make_request(url, headers = None, max_retries = 5, break_codes = None):

    """Retry request `max_retries` times, while catching arbitrary exceptions.

    Parameters
    ----------
    url : str
        URL of content that is being requested
    headers : dict or None
        Dictionary of key-value pairs for request headers
    max_retries : int
        Maximum number of times to retry the request 
    break_codes : list or None
        List of acceptable status codes that indicate that the request should 
        not be retried further. Useful if, for example, a `404` is expected at 
        some point to terminate a loop, and we don't want to retry to get the 
        404-ed page multiple times.

    Returns
    -------
    requests.Response or None
        Reponse from the request, or None if all retries failed.
    """

    if break_codes is None:
        break_codes = []

    r = None

    for n_retries in range(max_retries):
        try:
            r = request_until_200(
                url = url, 
                headers = headers, 
                max_retries = max_retries,
                break_codes = break_codes)
            logger.debug(f"Request for url: {url} succeeded on attempt: {n_retries}/{max_retries}")
        except Exception as e:
            logger.warning(f"Request for url: {url} raised exception: [{e}] on attempt: {n_retries}/{max_retries}")
            continue 
        else:
            break 
    else:
        logger.error(f"Request for url: {url} failed after {max_retries} attempts")

    return r

def request_until_200(url, headers = None, max_retries = 5, break_codes = None):

    """Retry request `max_retries` times, or until the request is successful.
    """

    if break_codes is None:
        break_codes = [200]
    else:
        break_codes = break_codes + [200]

    n_retries = 0
    r = requests.get(url, headers = headers)

    while r.status_code not in break_codes and n_retries < 5:
        logger.warning(f"Request for url: {url} returned status: {r.status_code} on attempt: {n_retries}/{max_retries}")
        n_retries += 1

        # back off subsequent requests
        time.sleep(n_retries)
        r = requests.get(url, headers = headers)

    if r.status_code not in break_codes:
        raise ValueError(f"Request for url: {url} failed with status: {r.status_code} after {max_retries} attempts")

    return r