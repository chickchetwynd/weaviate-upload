import os
import json
import weaviate
from datetime import datetime
from dotenv import load_dotenv
from weaviate.auth import AuthApiKey
from weaviate.classes.config import Property, Configure, DataType

# Load environment variables from .env.local
load_dotenv(".env.local")

# Environment Variables or Defaults
WEAVIATE_CLUSTER_URL = os.getenv('WEAVIATE_CLUSTER_URL')
WEAVIATE_API_KEY = os.getenv('WEAVIATE_API_KEY')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')

# Validate API Keys
if not WEAVIATE_CLUSTER_URL:
    raise ValueError("Missing 'WEAVIATE_CLUSTER_URL' in environment variables.")
if not WEAVIATE_API_KEY:
    raise ValueError("Missing 'WEAVIATE_API_KEY' in environment variables.")
if not OPENAI_API_KEY:
    raise ValueError("Missing 'OPENAI_API_KEY' in environment variables.")

# Connect to Weaviate
client = weaviate.connect_to_weaviate_cloud(
    cluster_url=WEAVIATE_CLUSTER_URL,
    auth_credentials=AuthApiKey(api_key=WEAVIATE_API_KEY),
    headers={"X-OpenAI-Api-Key": OPENAI_API_KEY}
)

# Ensure connection
if client.is_connected():
    print("Connected to Weaviate successfully!")

# Delete existing 'Candidate' collection if it exists
try:
    client.collections.delete("Candidate")
    print("Deleted existing 'Candidate' collection.")
except Exception as e:
    print("No existing 'Candidate' collection to delete.")

# Define the schema for the 'Candidate' collection
try:
    client.collections.create(
        name="Candidate",
        properties=[
            Property(name="name", data_type=DataType.TEXT),
            Property(name="candidate_values", data_type=DataType.TEXT),
            Property(name="candidate_strengths", data_type=DataType.TEXT),
            Property(name="jobSearchenvironment", data_type=DataType.TEXT_ARRAY),
            Property(name="skills", data_type=DataType.TEXT_ARRAY),
            
            # Education as an array of objects
            Property(
                name="education", 
                data_type=DataType.OBJECT_ARRAY,
                nested_properties=[
                    Property(name="degree", data_type=DataType.TEXT),
                    Property(name="university_start_year", data_type=DataType.INT),
                    Property(name="university_end_year", data_type=DataType.INT),
                    Property(name="education_area", data_type=DataType.TEXT),
                    Property(name="school_name", data_type=DataType.TEXT)
                ]
            ),
            
            # Experiences as an array of objects
            Property(
                name="experiences", 
                data_type=DataType.OBJECT_ARRAY,
                nested_properties=[
                    Property(name="title", data_type=DataType.TEXT),
                    Property(name="employer", data_type=DataType.TEXT),
                    Property(name="description", data_type=DataType.TEXT),
                    Property(name="is_current", data_type=DataType.BOOL),
                    Property(name="start_date", data_type=DataType.DATE),
                    Property(name="left_date", data_type=DataType.DATE),
                    Property(name="duration_years", data_type=DataType.TEXT)
                ]
            ),
            
            # Locations as an array of objects
            Property(
                name="locations", 
                data_type=DataType.OBJECT_ARRAY,
                nested_properties=[
                    Property(name="country", data_type=DataType.TEXT),
                    Property(name="state", data_type=DataType.TEXT),
                    Property(name="city", data_type=DataType.TEXT)
                ]
            ),
            
            Property(name="willing_to_relocate", data_type=DataType.BOOL),
            Property(name="mentra_profile_link", data_type=DataType.TEXT),
            
            # Add the new candidate_activity object
            Property(
                name="candidate_activity",
                data_type=DataType.OBJECT,
                nested_properties=[
                    Property(name="account_age_days", data_type=DataType.TEXT),
                    Property(name="count_of_logins", data_type=DataType.TEXT),
                    Property(name="last_login", data_type=DataType.DATE)
                ]
            ),
        ],
        vectorizer_config=Configure.Vectorizer.text2vec_openai()
    )
    print("Created 'Candidate' collection with OpenAI vectorizer.")
except Exception as e:
    print(f"Error creating 'Candidate' collection: {e}")
    client.close()
    exit(1)

def parse_date(date_str):
    """Convert date string to proper format or return None if invalid"""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return None

def process_candidate(candidate_data):
    """Process and validate a single candidate's data"""
    try:
        # Process all education records
        education_list = candidate_data.get('education', [])
        processed_education = []
        for edu in education_list:
            if isinstance(edu, dict):
                processed_edu = {
                    "degree": edu.get("degree", ""),
                    "university_start_year": int(edu.get("university_start_year", 0)) or None,
                    "university_end_year": int(edu.get("university_end_year", 0)) or None,
                    "education_area": edu.get("education_area", ""),
                    "school_name": edu.get("school_name", "")
                }
                processed_education.append(processed_edu)

        # Process experiences
        experiences = candidate_data.get('experiences', [])
        processed_experiences = []
        for exp in experiences:
            processed_exp = {
                "title": exp.get("title", ""),
                "employer": exp.get("employer", ""),
                "description": exp.get("description", ""),
                "is_current": bool(exp.get("is_current", False)),
                "start_date": parse_date(exp.get("start_date")),
                "left_date": parse_date(exp.get("left_date")),
                "duration_years": str(exp.get("duration_years", ""))
            }
            processed_experiences.append(processed_exp)

        # Process locations
        locations = candidate_data.get('locations', [])
        processed_locations = []
        for loc in locations:
            processed_loc = {
                "country": loc.get("country", ""),
                "state": loc.get("state", ""),
                "city": loc.get("city", "")
            }
            processed_locations.append(processed_loc)

        # Process candidate_activity
        candidate_activity = candidate_data.get('candidate_activity', {})
        processed_activity = {
            "account_age_days": str(candidate_activity.get("account_age_days", "")),
            "count_of_logins": str(candidate_activity.get("count_of_logins", "")),
            "last_login": parse_date(candidate_activity.get("last_login"))
        }

        # Construct the final candidate object
        return {
            "name": candidate_data.get("name", ""),
            "candidate_values": candidate_data.get("candidate_values", ""),
            "candidate_strengths": candidate_data.get("candidate_strengths", ""),
            "jobSearchenvironment": candidate_data.get("jobSearchenvironment", []),
            "skills": candidate_data.get("skills", []),
            "education": processed_education,
            "experiences": processed_experiences,
            "locations": processed_locations,
            "willing_to_relocate": bool(candidate_data.get("willing_to_relocate", False)),
            "mentra_profile_link": candidate_data.get("mentra_profile_link", ""),
            "candidate_activity": processed_activity
        }
    except Exception as e:
        print(f"Error processing candidate {candidate_data.get('name', 'Unknown')}: {e}")
        raise

def read_json_file(filepath):
    """Read JSON file with multiple objects, one per line"""
    data = []
    with open(filepath, 'r') as file:
        for line_number, line in enumerate(file, 1):
            try:
                # Parse each line as a separate JSON object
                json_obj = json.loads(line.strip())
                data.append(json_obj)
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON at line {line_number}")
                print(f"Line content: {line[:200]}...")
                print(f"Error details: {str(e)}")
                raise  # Re-raise the exception to stop processing
    
    print(f"Successfully parsed {len(data)} candidate records")
    return data

# Load the data
try:
    candidates = read_json_file('mentra_data.json')
except json.JSONDecodeError:
    print("Failed to parse JSON file. Stopping execution.")
    raise

try:
    for candidate_data in candidates:
        processed_candidate = process_candidate(candidate_data)
        
        if processed_candidate:
            try:
                # Insert candidate into collection
                uuid = client.collections.get("Candidate").data.insert(processed_candidate)
                print(f"Inserted: {processed_candidate['name']} with UUID: {uuid}")
            except Exception as e:
                print(f"Error inserting {processed_candidate['name']}: {e}")
        else:
            print(f"Skipping invalid candidate data")

except Exception as e:
    print(f"Unexpected error: {e}")
finally:
    # Close the client connection
    client.close()
    print("Connection to Weaviate closed.")
