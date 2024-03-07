# app/eu_scraper.py
import logging
import re
import asyncio
import aiohttp
import json
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures

# local imports
from .utils.page_fetcher import fetch_page, fetch_pages
from .utils.data_utils import get_trial_data_from_card, push_list_to_dynamodb, upload_jsons_to_s3, update_record, push_to_dynamodb
from .parsers.card_parser import CardParser
from .parsers.result_parser import ResultParser
from .parsers.protocol_parser import ProtocolParser

class EUClinicalTrialsScraper:
    """
    A scraper for extracting clinical trials data from the EU Clinical Trials Register.

    Attributes:
        start_page (int): The starting page number for scraping search results.
        end_page (int): The ending page number for scraping search results.
        start_date (str): The start date for filtering trials by date.
        end_date (str): The end date for filtering trials by date.
        base_url (str): The base URL of the EU Clinical Trials Register.
    """

    def __init__(self, start_page=None, end_page=None, start_date=None, end_date=None):
        """
        Initialize the scraper with optional parameters for specifying the scraping criteria.

        Args:
            start_page (int): The starting page number for scraping search results.
            end_page (int): The ending page number for scraping search results.
            start_date (str): The start date for filtering trials by date.
            end_date (str): The end date for filtering trials by date.
        """
        self.start_page = start_page
        self.end_page = end_page
        self.start_date = start_date
        self.end_date = end_date
        self.base_url = 'https://www.clinicaltrialsregister.eu/ctr-search/search?query='
    
    async def scrape(self):
        """
        Initiates the scraping process based on the provided criteria.
        """
        if self.start_date and self.end_date:
            await self.scrape_by_date()
        elif self.start_page and self.end_page:
            await self.scrape_by_page()
        else:
            raise Exception("Invalid arguments. Please provide either a date range or a page range")

    async def scrape_by_date(self):
        """
        Scrapes trials based on the provided date range.
        """
        search_url = f"{self.base_url}&dateFrom={self.start_date}&dateTo={self.end_date}"
        response = await fetch_page(search_url)
        response = await response.text()
        initial_search_page = BeautifulSoup(response, "html.parser")
        pages, results = self.get_num_pages_and_results(initial_search_page)
        search_urls = [f"{search_url}&page={page}" for page in range(1, pages + 1)]
        search_responses = await fetch_pages(search_urls)
        cards = await self.get_cards(search_responses)
        cards_data = self.parse_cards(cards)
        card_rows = [get_trial_data_from_card(card) for card in cards_data]
        logging.info(f"Extracted {len(card_rows)} rows from cards")
        await push_list_to_dynamodb(card_rows)
        logging.info(f"Pushed {len(card_rows)} items to DDB")
        await upload_jsons_to_s3(cards_data, "cards_data")
        logging.info(f"Pushed {len(cards_data)} items to S3")
        successful_trials = 0
        for i, card in enumerate(cards_data):
            trial_data = await self.get_trial_data(card)
            if not trial_data:
                logging.info(f"Failed to retrieve trial data for trial: {card['trial_id']}\tURL: {card['trial_results_link']}")
                continue
            new_record = update_record(trial_data)
            logging.info(f"Updating record {new_record['trial_id']} in DDB")
            await push_list_to_dynamodb([new_record])
            await upload_jsons_to_s3([trial_data], "trial_data")
            logging.info(f"Number of trials processed: {i + 1}")
            successful_trials += 1
        logging.info(f"Number of trials successfully processed: {successful_trials} out of {len(cards_data)}")

    def get_num_pages_and_results(self, initial_search_page):
        """
        Extracts the number of pages and results from the initial search page.

        Args:
            initial_search_page (BeautifulSoup): The BeautifulSoup object of the initial search page.

        Returns:
            tuple: A tuple containing the number of pages and results.
        """
        try:
            data_section = initial_search_page.find("div", {"id": "tabs-1"}).find("div", {"class": "outcome"})
            if not data_section:
                return None
            pattern = r"(\d+) result\(s\) found.*page \d+ of (\d+)"
            text = re.sub(r'\s+', ' ', data_section.text.strip().replace("  ", "").replace(",", "")).strip()
            match = re.search(pattern, text)
            if not match:
                return None
            results = match.group(1)
            pages = match.group(2)
            return int(pages), int(results)
        except Exception as e:
            raise Exception(f"Failed to determine number of pages and results: {str(e)}")

    async def scrape_by_page(self):
        """
        Scrapes trials based on the provided page range.
        """
        search_results = await self.scrape_search_pages()
        cards = await self.get_cards(search_results)
        cards_data =  self.parse_cards(cards)
        card_rows = [get_trial_data_from_card(card) for card in cards_data]
        logging.info(f"Extracted {len(card_rows)} rows from cards")
        await push_list_to_dynamodb(card_rows)
        logging.info(f"Pushed {len(card_rows)} items to DDB")
        await upload_jsons_to_s3(cards_data, "cards_data")
        logging.info(f"Pushed {len(cards_data)} items to S3")
        successful_trials = 0
        for i, card in enumerate(cards_data):
            trial_data = await self.get_trial_data(card)
            if not trial_data:
                logging.info(f"Failed to retrieve trial data for trial: {card['trial_id']}\tURL: {card['trial_results_link']}")
                continue
            new_record = update_record(trial_data)
            logging.info(f"Updating record {new_record['trial_id']} in DDB")
            await push_list_to_dynamodb([new_record])
            await upload_jsons_to_s3([trial_data], "trial_data")
            logging.info(f"Number of trials processed: {i + 1}")
            successful_trials += 1
        logging.info(f"Number of trials successfully processed: {successful_trials} out of {len(cards_data)}")

    async def get_trial_data(self, card_data):
        """
        Retrieves additional data for a trial based on the trial card data.

        Args:
            card_data (dict): The trial card data.

        Returns:
            dict: The complete trial data.
        """
        try:
            trial_data = {}
            trial_data["card"] = card_data
            trial_data["trial_id"] = card_data["trial_id"]
            protocols_urls = [protocol["protocol_url"] for protocol in trial_data["card"]["trial_protocols"] if protocol["protocol_url"]]
            trial_data["protocols"] = await self.get_protocols_data(protocols_urls)
            results_url = trial_data["card"]["trial_results_link"]
            if results_url:
                trial_data["results"] = await self.get_results(results_url)
            return trial_data
        except Exception as e:
            logging.error(f"Failed to retrieve trial data for trial: {trial_data['trial_id']} ---- Error: {str(e)}")
            return None
        
    async def get_protocols_data(self, protocols_urls):
        """
        Retrieves protocol data for a list of protocol URLs.

        Args:
            protocols_urls (list): A list of protocol URLs.

        Returns:
            list: A list of protocol data.
        """
        protocols = []
        for protocol_url in protocols_urls:
            try:
                response = await fetch_page(protocol_url)
                response = await response.text()
                soup = BeautifulSoup(response, "html.parser")
                protocol_data = {"url": protocol_url}
                protocol_parser = ProtocolParser(soup)
                protocol_data_up = protocol_parser.parse()
                protocol_data.update(protocol_data_up)
                protocols.append(protocol_data)
            except Exception as e:
                logging.error(f"Failed to retrieve protocol data: {protocol_url} ---- Error: {str(e)}")
        return protocols

    async def get_results(self, results_url):
        """
        Retrieves results data from a given results URL.

        Args:
            results_url (str): The URL to fetch results data from.

        Returns:
            dict: The results data.
        """
        try:
            response = await fetch_page(results_url)
            response = await response.text()
            soup = BeautifulSoup(response, "html.parser")
            result_parser = ResultParser(soup, url=results_url)
            return await result_parser.parse()
        except Exception as e:
            raise Exception(f"Failed to retrieve results data: {str(e)}")

    async def scrape_search_pages(self):
        """
        Scrapes the search results pages within the specified range.

        Returns:
            list: A list of search result page responses.
        """
        search_urls = [f"{self.base_url}&page={page}" for page in range(self.start_page, self.end_page + 1)]
        responses = await fetch_pages(search_urls)
        search_results = [response for response in responses if not isinstance(response, Exception)]
        return search_results

    async def get_cards(self, search_results):
        """
        Extracts trial cards from search result pages.

        Args:
            search_results (list): A list of search result page responses.

        Returns:
            list: A list of trial card elements.
        """
        cards = []
        for i, page in enumerate(search_results):
            logging.info(f"Scraping page {i + 1} of {len(search_results)}")
            text = await page.text()
            soup = BeautifulSoup(text, "html.parser")
            data_section = soup.find("div", {"id": "tabs"})
            cards.extend([card for card in data_section.find_all("table", {"class": "result"})])
        print(f"Total cards found: {len(cards)}")
        return cards
    
    def parse_card(self, card):
        """
        Parses a single trial card element.

        Args:
            card: A trial card element.

        Returns:
            dict: Parsed trial card data.
        """
        parser = CardParser(card)
        return parser.parse()

    def parse_cards(self, cards):
        """
        Parses a list of trial card elements.

        Args:
            cards (list): A list of trial card elements.

        Returns:
            list: Parsed trial card data.
        """
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(self.parse_card, card) for card in cards]
            parsed_data = [future.result() for future in concurrent.futures.as_completed(futures)]
        return parsed_data
