import os
import json
import weaviate
from dotenv import load_dotenv
from weaviate.auth import AuthApiKey

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
    }
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
        
        # Validate year is reasonable (between 1900 and current year)
        year = int(date_str.split('-')[0])
        if year < 1900 or year > 2024:
            print(f"Skipping invalid year in date: {date_str}")
            return None
            
        return f"{date_str}T00:00:00Z"
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
        batch = client.batch.configure(batch_size=100)
        inserted_count = 0
        
        with batch:
            for line in file:
                try:
                    candidate = json.loads(line.strip())
                    formatted_candidate = format_candidate_data(candidate)
                    client.batch.add_data_object(
                        data_object=formatted_candidate,
                        class_name="Candidate"
                    )
                    inserted_count += 1
                    print(f"Processing {inserted_count}: {candidate.get('name', 'Unknown')}")
                except Exception as e:
                    print(f"Error processing candidate: {e}")

        # Verify the insertion
        result = client.query.get(
            "Candidate", 
            ["name"]  # Specify at least one property to return
        ).with_limit(25000).do()  # Increase limit to cover all records
        actual_count = len(result['data']['Get']['Candidate'])
        print(f"\nSummary:")
        print(f"Processed: {inserted_count}")
        print(f"Actually inserted: {actual_count}")

    # Detailed verification
    print("\nVerifying data in Weaviate...")
    
    # Count total records
    result = client.query.get(
        "Candidate", 
        ["name"]
    ).with_limit(25000).do()
    actual_count = len(result['data']['Get']['Candidate'])
    
    # Sample some records to verify data quality
    sample = client.query.get(
        "Candidate", 
        ["name", "skills", "experiences {title employer}", "candidate_activity {last_login}"]
    ).with_limit(5).do()
    
    print(f"\nSummary:")
    print(f"Processed in this run: {inserted_count}")
    print(f"Total records in Weaviate: {actual_count}")
    print(f"\nSample of records in Weaviate:")
    for candidate in sample['data']['Get']['Candidate']:
        print(f"\nName: {candidate['name']}")
        print(f"Skills: {', '.join(candidate['skills'][:3])}...")
        if candidate['experiences']:
            print(f"Latest role: {candidate['experiences'][0]['title']} at {candidate['experiences'][0]['employer']}")
        if candidate['candidate_activity']:
            print(f"Last login: {candidate['candidate_activity']['last_login']}")
    print("\nData import completed.")

except FileNotFoundError:
    print("mentra_data.json file not found")
    raise
