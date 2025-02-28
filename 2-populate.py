import os
import json
import weaviate
from dotenv import load_dotenv
from weaviate.auth import AuthApiKey
from datetime import datetime

# Load environment variables from .env
load_dotenv(".env")

# Environment Variables
WEAVIATE_CLUSTER_URL = os.getenv('WEAVIATE_CLUSTER_URL')
WEAVIATE_API_KEY = os.getenv('WEAVIATE_API_KEY')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')

# Validate API Keys
if not all([WEAVIATE_CLUSTER_URL, WEAVIATE_API_KEY, OPENAI_API_KEY]):
    raise ValueError("Missing required environment variables")

# Connect to Weaviate
client = weaviate.Client(
    url=WEAVIATE_CLUSTER_URL,
    auth_client_secret=AuthApiKey(api_key=WEAVIATE_API_KEY),
    additional_headers={
        "X-OpenAI-Api-Key": OPENAI_API_KEY
    },
    timeout_config=(5, 60)  # (connect timeout, read timeout) in seconds
)

# Verify connection
try:
    client.schema.get()
    print("Connected to Weaviate successfully!")
except Exception as e:
    print(f"Failed to connect to Weaviate: {e}")
    raise

# Define the schema for the Candidate class
schema = {
    "class": "Candidate",
    "vectorizer": "text2vec-openai",
    "moduleConfig": {
        "text2vec-openai": {
            "model": "ada",
            "modelVersion": "002",
            "type": "text"
        }
    },
    "properties": [
        {"name": "name", "dataType": ["text"]},
        {"name": "contact_info", "dataType": ["object"], "nestedProperties": [
            {"name": "phone", "dataType": ["text"]},
            {"name": "email", "dataType": ["text"]}
        ]},
        {"name": "candidate_values", "dataType": ["text"]},
        {"name": "candidate_strengths", "dataType": ["text"]},
        {"name": "jobSearchenvironment", "dataType": ["text[]"]},
        {"name": "skills", "dataType": ["text[]"]},
        {"name": "education", "dataType": ["object[]"], "nestedProperties": [
            {"name": "degree", "dataType": ["text"]},
            {"name": "university_start_year", "dataType": ["int"]},
            {"name": "university_end_year", "dataType": ["int"]},
            {"name": "education_area", "dataType": ["text"]},
            {"name": "school_name", "dataType": ["text"]}
        ]},
        {"name": "experiences", "dataType": ["object[]"], "nestedProperties": [
            {"name": "title", "dataType": ["text"]},
            {"name": "employer", "dataType": ["text"]},
            {"name": "description", "dataType": ["text"]},
            {"name": "is_current", "dataType": ["boolean"]},
            {"name": "start_date", "dataType": ["date"]},
            {"name": "left_date", "dataType": ["date"]},
            {"name": "duration_years", "dataType": ["text"]}
        ]},
        {"name": "locations", "dataType": ["object[]"], "nestedProperties": [
            {"name": "country", "dataType": ["text"]},
            {"name": "state", "dataType": ["text"]},
            {"name": "city", "dataType": ["text"]}
        ]},
        {"name": "willing_to_relocate", "dataType": ["boolean"]},
        {"name": "mentra_profile_link", "dataType": ["text"]},
        {"name": "candidate_activity", "dataType": ["object"], "nestedProperties": [
            {"name": "account_age_days", "dataType": ["text"]},
            {"name": "count_of_logins", "dataType": ["text"]},
            {"name": "last_login", "dataType": ["date"]}
        ]}
    ]
}

# Delete existing schema if it exists
try:
    client.schema.delete_class("Candidate")
    print("Deleted existing 'Candidate' class.")
except Exception:
    print("No existing 'Candidate' class to delete.")

# Create schema
client.schema.create_class(schema)
print("Created 'Candidate' class with OpenAI vectorizer.")

def format_date(date_str):
    """Format dates to RFC3339 and validate"""
    if not date_str:
        return None
    try:
        # Fix two-digit years
        if len(date_str.split('-')[0]) == 2:
            date_str = f"20{date_str}"
        
        # Split the date parts
        parts = date_str.split('-')
        
        # Handle different date formats
        if len(parts) == 1:  # Just year
            date_str = f"{date_str}-01-01"
        elif len(parts) == 2:  # Year and month
            date_str = f"{date_str}-01"
        
        # Validate year is reasonable (between 1900 and 5 years from now)
        current_year = datetime.now().year
        max_year = current_year + 5
        year = int(parts[0])
        if year < 1900 or year > max_year:
            print(f"Skipping invalid year in date: {date_str}")
            return None
            
        # Return RFC3339 formatted date
        return f"{date_str}T00:00:00+00:00"
    except Exception as e:
        print(f"Error processing date {date_str}: {e}")
        return None

def format_candidate_data(candidate):
    """Format data to match Weaviate requirements"""
    
    # Process experiences dates
    if 'experiences' in candidate:
        for exp in candidate['experiences']:
            if 'start_date' in exp:
                exp['start_date'] = format_date(exp['start_date'])
            if 'left_date' in exp:
                exp['left_date'] = format_date(exp['left_date'])
    
    # Process candidate_activity
    if 'candidate_activity' in candidate:
        activity = candidate['candidate_activity']
        if 'account_age_days' in activity:
            activity['account_age_days'] = str(activity['account_age_days'])
        if 'count_of_logins' in activity:
            activity['count_of_logins'] = str(activity['count_of_logins'])
        if 'last_login' in activity:
            activity['last_login'] = format_date(activity['last_login'])
    
    return candidate

# Load and insert the data
try:
    with open('mentra_data.json', 'r') as file:
        # Configure batch settings for better reliability
        batch = client.batch(
            batch_size=100,
            dynamic=True,  # Dynamically adjust batch size based on performance
            timeout_retries=3  # Simple retry configuration
        )
        inserted_count = 0
        
        for line in file:
            try:
                candidate = json.loads(line.strip())
                formatted_candidate = format_candidate_data(candidate)
                batch.add_data_object(
                    data_object=formatted_candidate,
                    class_name="Candidate"
                )
                inserted_count += 1
                print(f"Processing {inserted_count}: {candidate.get('name', 'Unknown')}")
            except Exception as e:
                print(f"Error processing candidate: {str(e)}")

        # Verify the insertion using aggregation
        result = client.query.aggregate("Candidate").with_meta_count().do()
        actual_count = result['data']['Aggregate']['Candidate'][0]['meta']['count']
        
        print(f"\nSummary:")
        print(f"Processed in this run: {inserted_count}")
        print(f"Total records in Weaviate: {actual_count}")

except FileNotFoundError:
    print("mentra_data.json file not found")
    raise
