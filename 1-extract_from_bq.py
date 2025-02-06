from google.cloud import bigquery
import json
import os


def initialize_client():
    """Initialize BigQuery client using credentials"""
    try:
        client = bigquery.Client.from_service_account_json('key.json')
        return client
    except Exception as e:
        print(f"Error initializing BigQuery client: {e}")
        raise

def execute_query(client):
    """Execute BigQuery query and return results"""
    query = """
    SELECT
        *
    FROM
        `mentra-398813.analytics.candidate_enriched_ai`
    """
    
    try:
        query_job = client.query(query)
        return query_job.result()
    except Exception as e:
        print(f"Error executing query: {e}")
        raise

def convert_row(row):
    """Convert BigQuery row to JSON-serializable dictionary"""
    row_dict = dict(row)
    converted = {}

    # Handle each field according to its known type
    for key, value in row_dict.items():
        if key in ['jobSearchenvironment', 'skills']:
            # Simple arrays of strings
            converted[key] = value if value else []
        elif key in ['education', 'experiences', 'locations']:
            # Arrays of structs
            converted[key] = value or []
        elif key == 'candidate_activity':
            # Single struct
            converted[key] = value if value else {}
        else:
            # Simple scalar values
            converted[key] = value if value is not None else ""

    return converted

def save_to_json(data, filepath='mentra_data.json'):
    """Save data to JSON file, writing each record on a new line"""
    try:
        with open(filepath, 'w') as f:
            for record in data:
                # Write each record as a single line of JSON
                f.write(json.dumps(record) + '\n')
        print(f"Data successfully exported to {filepath}")
    except Exception as e:
        print(f"Error saving to JSON: {e}")
        raise

def main():
    try:
        # Initialize client
        client = initialize_client()
        
        # Execute query
        results = execute_query(client)
        
        # Convert results to list of dictionaries
        data = [convert_row(row) for row in results]
        
        # Save to JSON
        save_to_json(data)
        
        print(f"Successfully processed {len(data)} records")
        
    except Exception as e:
        print(f"Pipeline failed: {e}")
        raise
    finally:
        client.close()

if __name__ == "__main__":
    main()
