# app/utils/args_parser.py
from datetime import datetime
import argparse
import sys

def validate_dates(start_date, end_date):
    """
    Validates and converts start and end dates from strings to date objects.
    
    Ensures that the start date is before the end date. Raises an error if the
    format is incorrect or the start date comes after the end date.

    Args:
        start_date (str): Start date in 'YYYY-MM-DD' format.
        end_date (str): End date in 'YYYY-MM-DD' format.

    Returns:
        tuple: A tuple containing the start and end dates as date objects.

    Raises:
        argparse.ArgumentTypeError: If there's a problem with the date format or logic.
    """
    try:
        start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
        if start_date > end_date:
            raise ValueError("Start date cannot be after end date.")
    except ValueError as e:
        raise argparse.ArgumentTypeError(f"Date error: {e}")
    return start_date, end_date

def validate_args(args):
    """
    Validates the command-line arguments provided by the user.

    Ensures that either date range or page range is provided, but not both,
    and that if a range is provided, both start and end points are included.

    Args:
        args (Namespace): Parsed command-line arguments.

    Returns:
        Namespace: The original arguments, potentially with dates converted to date objects.

    Exits:
        If validation fails, the program will exit with an error message.
    """
    if args.start_date is None and args.end_date is None and args.start_page is None and args.end_page is None:
        print("You must provide either start/end dates or start/end pages.")
        sys.exit(1)
    elif (args.start_date is not None or args.end_date is not None) and (args.start_page is not None or args.end_page is not None):
        print("You must provide either start/end dates or start/end pages, not both.")
        sys.exit(1)
    elif (args.start_date is not None and args.end_date is None) or (args.start_date is None and args.end_date is not None):
        print("Both start and end dates must be provided.")
        sys.exit(1)
    elif (args.start_page is not None and args.end_page is None) or (args.start_page is None and args.end_page is not None):
        print("Both start and end pages must be provided.")
        sys.exit(1)
    elif args.start_date and args.end_date:
        args.start_date, args.end_date = validate_dates(args.start_date, args.end_date)
    return args

def parse_arguments():
    """
    Parses command-line arguments for date and page ranges.

    Defines and groups arguments for specifying start and end dates or pages,
    validates the arguments, and returns them for further processing.

    Returns:
        Namespace: The parsed command-line arguments.
    """
    parser = argparse.ArgumentParser(description='Process some arguments.')
    
    date_group = parser.add_argument_group('Date range')
    date_group.add_argument('--start-date', help='Start date (YYYY-MM-DD)', type=str)
    date_group.add_argument('--end-date', help='End date (YYYY-MM-DD)', type=str)
    
    page_group = parser.add_argument_group('Page range')
    page_group.add_argument('--start-page', help='Start page number', type=int)
    page_group.add_argument('--end-page', help='End page number', type=int)

    args = parser.parse_args()
    validate_args(args)
    
    return args
