# app/utils/page_fetcher.py

import asyncio
import aiohttp
import logging
from aiohttp_client_cache import SQLiteBackend, CachedSession

async def fetch_pdf(pdf_link, session=None):
    """
    Fetches a PDF document from the specified URL asynchronously.

    Args:
        pdf_link (str): The URL of the PDF document to fetch.
        session (aiohttp.ClientSession, optional): An existing aiohttp ClientSession instance. 
            If not provided, a new session will be created.

    Returns:
        aiohttp.ClientResponse: The response object containing the PDF document.
    """
    if session is None:
        cache = SQLiteBackend(cache_name='request_cache')
        async with CachedSession(cache=cache) as new_session:
            return await _make_request(new_session, pdf_link)
    else:
        return await _make_request(session, pdf_link)

async def fetch_page(url, session=None):
    """
    Fetches a web page from the specified URL asynchronously.

    Args:
        url (str): The URL of the web page to fetch.
        session (aiohttp.ClientSession, optional): An existing aiohttp ClientSession instance. 
            If not provided, a new session will be created.

    Returns:
        aiohttp.ClientResponse: The response object containing the web page content.
    """
    if session is None:
        cache = SQLiteBackend(cache_name='request_cache')
        async with CachedSession(cache=cache) as new_session:
            return await _make_request(new_session, url)
    else:
        return await _make_request(session, url)

async def fetch_pages(urls, session=None):
    """
    Fetches multiple web pages asynchronously from the specified URLs.

    Args:
        urls (List[str]): A list of URLs of the web pages to fetch.
        session (aiohttp.ClientSession, optional): An existing aiohttp ClientSession instance. 
            If not provided, a new session will be created.

    Returns:
        List[aiohttp.ClientResponse]: A list of response objects containing the web page contents.
    """
    if session is None:
        cache = SQLiteBackend(cache_name='request_cache')
        async with CachedSession(cache=cache) as new_session:
            return await _fetch_multiple(new_session, urls)
    else:
        return await _fetch_multiple(session, urls)

async def _make_request(session, url, attempt=1, max_attempts=7):
    """
    Internal function to make a request to a URL with retry logic.

    Args:
        session (aiohttp.ClientSession): The aiohttp ClientSession instance.
        url (str): The URL to make the request to.
        attempt (int): The current attempt number.
        max_attempts (int): The maximum number of retry attempts.

    Returns:
        aiohttp.ClientResponse: The response object containing the result of the request.

    Raises:
        Exception: If the request fails after the maximum number of retry attempts.
    """
    try:
        async with session.get(url) as response:
            if response.status == 200:
                return response
            else:
                raise Exception(f"Failed to fetch {url} with status code: {response.status}")
    except Exception as e:
        if attempt < max_attempts:
            print(f"Attempt {attempt} failed for {url}. Error: {e}. Retrying...")
            logging.error(f"Attempt {attempt} failed for {url}. Error: {e}. Retrying...")
            await asyncio.sleep(2 ** attempt)  
            return await _make_request(session, url, attempt + 1, max_attempts)
        else:
            raise Exception(f"Failed to fetch {url} after {max_attempts} attempts. Last error: {e}")

async def _fetch_multiple(session, urls):
    """
    Internal function to fetch multiple web pages asynchronously.

    Args:
        session (aiohttp.ClientSession): The aiohttp ClientSession instance.
        urls (List[str]): A list of URLs of the web pages to fetch.

    Returns:
        List[aiohttp.ClientResponse]: A list of response objects containing the web page contents.
    """
    tasks = [fetch_page(url, session) for url in urls]
    responses = await asyncio.gather(*tasks, return_exceptions=True)
    return responses
