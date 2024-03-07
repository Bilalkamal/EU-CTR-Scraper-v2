# app/constants.py

# Time in seconds between requests
REQUEST_DELAY = 10

# Maximum backoff time in seconds (5 minutes)
MAX_BACKOFF_TIME = 300

# Base URL for the EU Clinical Trials Register
BASE_URL = "https://www.clinicaltrialsregister.eu/"

# Custom headers for HTTP requests
HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36"
}

# Columns for the database table
COLUMNS = ["trial_id", "title", "url", "acronym", "status", "summary", "results", "conditions", "interventions", "primary_outcome",
"secondary_outcome", "other_outcome", "sponsor", "collaborators", "sex", "age", "phases", "enrollment", "funder_type",
"study_type", "study_design", "other_ids", "start_date", "primary_completion_date", "completion_date", "first_posted",
"results_first_posted", "last_update_posted", "locations", "study_documents", "protocols"]

