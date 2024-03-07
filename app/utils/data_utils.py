# app/utils/data_utils.py

import aioboto3
from boto3.dynamodb.types import TypeSerializer
import logging
import json
import pdfplumber
import zipfile
import re
from io import BytesIO

from .config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, AWS_DDB_TABLE_NAME, AWS_S3_BUCKET_NAME
from ..constants import COLUMNS


def get_trial_data_from_card(card_data):
    """
    Extracts data from a card and returns a dictionary with the data.

    Args:
        card_data (dict): A dictionary containing data from a card.

    Returns:
        dict: A dictionary containing the extracted data.
    """
    
    row_data = {column: None for column in COLUMNS}
    row_data["trial_id"] = card_data["trial_id"]
    row_data["title"] = card_data["full_title"]
    row_data["url"] = card_data["trial_results_link"]
    row_data["age"] = card_data["population_age"]
    row_data["sex"] = card_data["gender"]
    row_data["start_date"] = card_data["start_date"]
    row_data["sponsor"] = card_data["sponsor_name"]
    row_data["conditions"] = card_data["medical_condition"]
    row_data["disease"] = card_data["disease"]
    row_data["protocols"] = card_data["trial_protocols"]
    return row_data


def serialize_data(data):
    """
    Serializes data using TypeSerializer.

    Args:
        data (dict): The data to be serialized.

    Returns:
        dict: The serialized data.
    """
    serializer = TypeSerializer()
    return {key: serializer.serialize(value) for key, value in data.items()}


def search_for_key(json_data, target_key):
    """
    Searches for a key in nested JSON data.

    Args:
        json_data (dict or list): The JSON data to search through.
        target_key (str): The key to search for.

    Returns:
        Any: The value associated with the target key, if found, otherwise None.
    """
    if isinstance(json_data, dict):
        for key, value in json_data.items():
            if key == target_key:
                return value
            else:
                result = search_for_key(value, target_key)
                if result is not None:
                    return result
    elif isinstance(json_data, list):
        for item in json_data:
            result = search_for_key(item, target_key)
            if result is not None:
                return result
    return None

def collect_keys_with_keyword(json_data, keyword):
    """
    Collects keys containing a specific keyword from nested JSON data.

    Args:
        json_data (dict or list): The JSON data to search through.
        keyword (str): The keyword to search for in keys.

    Returns:
        list: A list of dictionaries containing keys and their corresponding values that contain the keyword.
    """
    keys_with_keyword = []
    
    def search_keys(data, parent_key=''):
        if isinstance(data, dict):
            for key, value in data.items():
                if re.search(keyword, key, re.IGNORECASE):
                    keys_with_keyword.append({key:value})
                search_keys(value, key if parent_key else key)
        elif isinstance(data, list):
            for i, item in enumerate(data):
                search_keys(item, f'[{i}]')
    
    search_keys(json_data)
    
    if keys_with_keyword:
        return keys_with_keyword
    else:
        return None

def update_record(trial_data):
    """
    Updates a record with additional data from trial data.

    Args:
        trial_data (dict): Data from a trial.

    Returns:
        dict: The updated record data.
    """
    row_data = {column: None for column in COLUMNS}
    extract_card_data(trial_data, row_data)
    
    if len(trial_data["protocols"]) == 0:
        return row_data
    
    extract_protocol_data(trial_data, row_data)
    
    if not "results" in trial_data.keys():
        return row_data
    extract_results_data(trial_data, row_data)
    return row_data

def extract_results_data(trial_data, row_data):
    """
    Extracts results data from trial data.

    Args:
        trial_data (dict): Data from a trial.
        row_data (dict): Dictionary to store the extracted data.
    """
    current = [x for x in trial_data["results"] if "current" in x][0]
    current = trial_data["results"][current]
    if "global_end_date" in current["summary"]:
        completion_date = current["summary"]["global_end_date"]
        row_data["completion_date"] = completion_date[0] if isinstance(completion_date, list) else completion_date
    if "results_information" in current and "This version publication date" in current["results_information"]:
        last_update_posted = current["results_information"]["This version publication date"]
        row_data["last_update_posted"] = last_update_posted[0] if isinstance(last_update_posted, list) else last_update_posted
    if "results_information" in current and "First version publication date" in current["results_information"]:
        results_first_posted = current["results_information"]["First version publication date"]
        row_data["results_first_posted"] = results_first_posted[0] if isinstance(results_first_posted, list) else results_first_posted
    
    primary_completion_date = search_for_key(current, "Primary completion date")
    if primary_completion_date:
        row_data["primary_completion_date"] = primary_completion_date[0] if isinstance(primary_completion_date, list) else primary_completion_date
    
    enrollment = search_for_key(current, "Worldwide total number of subjects")
    if enrollment:
        row_data["enrollment"] = enrollment[0] if isinstance(enrollment, list) else enrollment
    
    extract_phase_data(trial_data, row_data)

def extract_phase_data(trial_data, row_data):
    """
    Extracts phase data from trial data.

    Args:
        trial_data (dict): Data from a trial.
        row_data (dict): Dictionary to store the extracted data.
    """
    phases = collect_keys_with_keyword(trial_data, "Phase")
    if phases:
        unique_dict = {}
        for item in phases:
            for key, value in item.items():
                if key not in unique_dict:
                    unique_dict[key] = value[0]
        phases = [{key: value} for key, value in unique_dict.items()]
        row_data["phases"] = phases

def safe_get(d, keys):
    """
    Safely retrieves a value from a nested dictionary using a list of keys.

    Args:
        d (dict): The nested dictionary.
        keys (list): A list of keys to traverse the dictionary.

    Returns:
        Any: The value associated with the provided keys, or None if not found.
    """
    assert isinstance(keys, list), "keys must be provided as a list"
    
    for key in keys:
        if isinstance(d, dict) and key in d:
            d = d[key]
        else:
            return None
    return d

def extract_protocol_data(trial_data, row_data):
    """
    Extracts protocol data from trial data.

    Args:
        trial_data (dict): Data from a trial.
        row_data (dict): Dictionary to store the extracted data.
    """
    protocol_info = safe_get(trial_data, ["protocols", 0, "A. Protocol Information"])
    if protocol_info:
        title = protocol_info.get("Full title of the trial")
        row_data["title"] = title[0] if isinstance(title, list) else title

    summary = safe_get(trial_data, ["protocols", 0, "summary"])
    if summary:
        if "Trial Status" in summary:
            status = summary["Trial Status"]
            row_data["status"] = status[0] if isinstance(status, list) else status
        if "Clinical Trial Type" in summary:
            study_type = summary["Clinical Trial Type"]
            row_data["study_type"] = study_type[0] if isinstance(study_type, list) else study_type
        if "Date on which this record was first entered in the EudraCT database" in summary:
            first_posted = summary["Date on which this record was first entered in the EudraCT database"]
            row_data["first_posted"] = first_posted[0] if isinstance(first_posted, list) else first_posted

    sponsor_info = safe_get(trial_data, ["protocols", 0, "B. Sponsor Information"])
    if sponsor_info:
        if "Country" in sponsor_info:
            country = sponsor_info["Country"]
            row_data["locations"] = country[0] if isinstance(country, list) else country
        if "Status of the sponsor" in sponsor_info:
            sponsor_status = sponsor_info["Status of the sponsor"]
            row_data["funder_type"] = sponsor_status[0] if isinstance(sponsor_status, list) else sponsor_status

def extract_card_data(trial_data, row_data):
    """
    Extracts card data from trial data.

    Args:
        trial_data (dict): Data from a trial.
        row_data (dict): Dictionary to store the extracted data.
    """
    card_data = trial_data["card"]
    row_data["trial_id"] = card_data["trial_id"]
    row_data["title"] = card_data["full_title"]
    row_data["url"] = card_data["trial_results_link"]
    row_data["age"] = card_data["population_age"]
    row_data["sex"] = card_data["gender"]
    row_data["start_date"] = card_data["start_date"]
    row_data["sponsor"] = card_data["sponsor_name"]
    row_data["conditions"] = card_data["medical_condition"]
    row_data["disease"] = card_data["disease"]
    row_data["protocols"] = card_data["trial_protocols"]
    row_data["status"] = card_data["trial_protocols"][0]["protocol_status"]


async def retrieve_record_from_dynamodb(record_id):
    """
    Retrieves a record from DynamoDB.

    Args:
        record_id (str): The ID of the record to retrieve.

    Returns:
        dict: The retrieved record data.
    """
    async with aioboto3.client(
        "dynamodb",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    ) as dynamodb_client:
        try:
            response = await dynamodb_client.get_item(
                TableName=AWS_DDB_TABLE_NAME,
                Key={"trial_id": {"S": record_id}}
            )
            return response.get("Item")
        except Exception as e:
            print(f"Failed to retrieve record with id {record_id}: {e}")
            return None

async def push_list_to_dynamodb(items, session=None):
    """
    Pushes a list of items to a DynamoDB table.

    Args:
        session (aiobotocore.session.AioSession): An aiohttp session.
        items (list): A list of dictionaries containing the items to be pushed to the DynamoDB table.

    Returns:
        None
    """
    if not session:
        session = aioboto3.Session()
    async with session.client("dynamodb", region_name=AWS_REGION, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY) as client:
        for i, item in enumerate(items):
            logging.info(f"Pushing item {i + 1} / {len(items)} to DDB")
            item = serialize_data(item)
            await client.put_item(TableName=AWS_DDB_TABLE_NAME, Item=item)
    return None


async def push_to_dynamodb(item, session=None):
    """
    Pushes a single item to a DynamoDB table.

    Args:
        session (aiobotocore.session.AioSession): An aiohttp session.
        item (dict): A dictionary containing the item to be pushed to the DynamoDB table.

    Returns:
        None
    """
    if not session:
        session = aioboto3.Session()
    async with session.client("dynamodb", region_name=AWS_REGION, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY) as client:
        serializer = TypeSerializer()
        item = {key: serializer.serialize(value) for key, value in item.items()}
        await client.put_item(TableName=AWS_DDB_TABLE_NAME, Item=item)

    return None

async def update_record_in_dynamodb(updated_record):
    """
    Updates a record in DynamoDB.

    Args:
        updated_record (dict): The updated record data.

    Returns:
        dict: The response from DynamoDB.
    """
    async with aioboto3.client(
        "dynamodb",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    ) as dynamodb_client:
        try:
            response = await dynamodb_client.put_item(
                TableName=AWS_DDB_TABLE_NAME,
                Item=updated_record
            )
            return response
        except Exception as e:
            print(f"Failed to update record with id {updated_record['trial_id']}: {e}")
            return None

async def create_directory_if_not_exists(s3_client, directory_name):

    try:
        await s3_client.head_object(Bucket=AWS_S3_BUCKET_NAME, Key=f"{directory_name}/")
    except s3_client.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            await s3_client.put_object(Bucket=AWS_S3_BUCKET_NAME, Key=f"{directory_name}/")
        else:
            raise e

async def upload_html_to_s3(html_content, directory_name, html_file_name):
    """
    Uploads HTML content to an S3 bucket.

    Args:
        html_content (str): The HTML content to upload.
        directory_name (str): The name of the directory in the S3 bucket.
        html_file_name (str): The name of the HTML file.

    Returns:
        None
    """
    async with aioboto3.client('s3',
                               aws_access_key_id=AWS_ACCESS_KEY_ID,
                               aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                               region_name=AWS_REGION) as s3_client:
        await create_directory_if_not_exists(s3_client, directory_name)
        s3_key = f"{directory_name}/{html_file_name}.html"
        await s3_client.put_object(Body=html_content, Bucket=AWS_S3_BUCKET_NAME, Key=s3_key)
        logging.info(f"HTML content uploaded to S3 bucket '{AWS_S3_BUCKET_NAME}' under directory '{directory_name}' with name '{html_file_name}'")


async def upload_json_to_s3(json_content, directory_name, json_file_name):
    """
    Uploads JSON content to an S3 bucket.

    Args:
        json_content (str): The JSON content to upload.
        directory_name (str): The name of the directory in the S3 bucket.
        json_file_name (str): The name of the JSON file.

    Returns:
        None
    """
    async with aioboto3.client('s3',
                               aws_access_key_id=AWS_ACCESS_KEY_ID,
                               aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                               region_name=AWS_REGION) as s3_client:
        await create_directory_if_not_exists(s3_client, directory_name)
        s3_key = f"{directory_name}/{json_file_name}.json"
        await s3_client.put_object(Body=json_content, Bucket=AWS_S3_BUCKET_NAME, Key=s3_key)
        logging.info(f"JSON content uploaded to S3 bucket '{AWS_S3_BUCKET_NAME}' under directory '{directory_name}' with name '{json_file_name}'")

async def upload_jsons_to_s3(data, name_prefix):
    """
    Uploads a list of JSON objects to an S3 bucket.

    Args:
        data (list): A list of dictionaries containing JSON data.
        name_prefix (str): The prefix to use for the file names.

    Returns:
        None
    """
    for item in data:
        content = json.dumps(item)
        directory_name = f"{item['trial_id']}"
        file_name = f"{name_prefix}-{item['trial_id']}"
        await upload_json_to_s3(content, directory_name, file_name)
    return None

async def upload_htmls_to_s3(data, name_prefix, urls=None):
    """
    Uploads a list of HTML content to an S3 bucket.

    Args:
        data (list): A list of HTML content (either as strings or aiohttp.ClientResponse objects).
        name_prefix (str): The prefix to use for the file names.
        urls (list, optional): A list of URLs corresponding to the HTML content.

    Returns:
        None
    """
    if not urls:
        urls = [None] * len(data)
    for i, item in enumerate(data):
        content = await item.text()
        trial_id = str(urls[i]).split("/")[-2]
        file_name = f"{name_prefix}-{trial_id}"
        await upload_html_to_s3(content, trial_id, file_name)
    return None


def extract_text_and_tables_from_pdf(zip_bytes):
    """
    Extracts text and tables from the first PDF file contained within a ZIP archive.

    Opens the ZIP archive from bytes, reads the first PDF file, and extracts all text
    and tables from every page using pdfplumber. The text is concatenated, and tables
    are collected in a list.

    Args:
        zip_bytes (bytes): The byte content of the ZIP file containing the PDF.

    Returns:
        tuple: A tuple containing all extracted text as a single string and a list of tables.
    """
    text = ""
    tables = []
    zip_in_memory = BytesIO(zip_bytes)

    with zipfile.ZipFile(zip_in_memory, 'r') as zip_ref:
        pdf_name = zip_ref.namelist()[0]
        with zip_ref.open(pdf_name) as pdf_file_in_zip:
            pdf_bytes = BytesIO(pdf_file_in_zip.read())
            with pdfplumber.open(pdf_bytes) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text + "\n"
                    page_tables = page.extract_tables()
                    for table in page_tables:
                        tables.append(table)
    return text, tables
