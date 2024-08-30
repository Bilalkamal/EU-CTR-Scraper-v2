# EU Clinical Trials Register Scraper

![Screenshot of Scraper Output](./Screenshot%202024-08-30%20at%2022.17.10.png)

## Introduction

This project is developed to facilitate the scraping of clinical trial data from the [EU Clinical Trials Register](https://www.clinicaltrialsregister.eu/). It's specifically designed to fetch detailed information about trials within a specified date range. The scraper navigates through the register, extracts data from individual trial records, and compiles the information into structured formats for further analysis or reporting. The data is stored in JSON files on AWS S3 and in a DynamoDB table.

## Project Structure

```
.
├── app
│   ├── __init__.py
│   ├── constants.py
│   ├── eu_scraper.py
│   ├── parsers
│   │   ├── __init__.py
│   │   ├── card_parser.py
│   │   ├── protocol_parser.py
│   │   └── result_parser.py
│   └── utils
│       ├── __init__.py
│       ├── args_parser.py
│       ├── config.py
│       ├── data_utils.py
│       ├── logger.py
│       └── page_fetcher.py
├── main.py
├── README.md
└── requirements.txt

5 directories, 11 files

```

## Setup and Installation

1. Clone the repository to your local machine.
2. Navigate to the project directory and install the necessary dependencies by running:

```bash
pip install -r requirements.txt
```
3. Set up the required environment variables for AWS access. The following environment variables are required:
   - `AWS_ACCESS_KEY_ID`: The access key for your AWS account.
   - `AWS_SECRET_ACCESS_KEY`: The secret access key for your AWS account.
   - `AWS_DEFAULT_REGION`: The default AWS region to use for the scraper.
   - `S3_BUCKET_NAME`: The name of the S3 bucket where the scraped data will be stored.
   - `DYNAMODB_TABLE_NAME`: The name of the DynamoDB table where the trial data will be stored.

## Usage

To start the scraping process, execute the `main.py` script with the required arguments for start and end dates. The dates should be in the YYYY-MM-DD format.
You can also scrape by page number by providing the start and end page numbers.

Example scraping by date range:

```bash
python3 main.py --start-date 2022-12-01 --end-date 2022-12-31
```


 ***Note:*** Ensure the start and end dates are valid and the start date is before the end date.


Example scraping by page number:

```bash
python3 main.py --start-page 1 --end-page 10
```


## Features

- **Comprehensive Data Extraction**: Targets all key data fields available on the EU Clinical Trials Register.
- **Custom Date Range and Page Number**: Allows users to specify the period for which the trial data should be fetched. Additionally, users can specify the page number to scrape.
- **Structured Output**: Organizes scraped data into a coherent structure, facilitating easy access and analysis.
- **AWS Integration**: Integrates with AWS services to store the scraped data in S3 and trial data in DynamoDB.
- **Error Handling**: Implements robust error handling to manage and log issues encountered during the scraping process.
- **Efficient Parsing**: Uses specialized parsers to extract and process trial details, protocol information, and results.

- **Logging and Output Formatting**: Provides detailed logging and output formatting to ensure transparency and traceability of the scraping process.


## Modules

- `eu_scraper.py`: The main scraper component that orchestrates the retrieval of trial listings and details from the EU Clinical Trials Register.
- `parsers/`: Contains parsers for different sections of the trial data, including trial cards, protocols, and results.
- `utils/`: Contains utility modules for argument parsing, configuration, data handling, logging, and page fetching.
- `constants.py`: Configuration file holding constants used across the project, such as base URLs and request headers.

## Output

The scraper generates two types of output:
1.  **JSON Files**: The scraped data is stored in JSON files, with each file containing the details of a single trial, including the HTML of the pages scraped.
<!-- according to a predefined structure. -->
2.  **DynamoDB**: The trial data is stored in a DynamoDB table, with each item representing a single trial.

## Future Improvements
- **Additional Search Features**: Allow users to specify additional search criteria, such as fetching only trials with available results.
- **Testing**: Implement comprehensive unit tests to ensure the scraper's functionality and robustness.
