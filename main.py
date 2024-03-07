# main.py
import asyncio
import logging
from app.eu_scraper import EUClinicalTrialsScraper
from app.utils.logger import setup_logging
from app.utils.args_parser import parse_arguments
import time

async def main():
    """
    Main entry point for the application.
    """
    setup_logging()
    start = time.time()
    args = parse_arguments()

    if args.start_date and args.end_date:
        logging.info(f"Processing by date range: {args.start_date} to {args.end_date}")
        start_date = args.start_date
        end_date = args.end_date 
        scraper = EUClinicalTrialsScraper(start_date = start_date, end_date = end_date)
        await scraper.scrape()
    elif args.start_page and args.end_page:
        logging.info(f"Processing by page range: {args.start_page} to {args.end_page}")
        start_page = args.start_page 
        end_page = args.end_page 
        scraper = EUClinicalTrialsScraper(start_page = start_page, end_page = end_page)
        await scraper.scrape()

    
    end = time.time()
    logging.info(f"Scraping complete.\tTime taken: {end - start:.2f} seconds")
    
    

if __name__ == "__main__":
    asyncio.run(main())
    """
    Example usage:
        Scrape by date: 
            python main.py --start-date 2021-01-01 --end-date 2021-01-31
        Scrape by page:
            python main.py --start-page 1 --end-page 10
    """
    