# data_ingestion_script.py

import pandas as pd
import requests
from datetime import datetime

def fetch_data(api_url):
    """
    Function to fetch data from an API endpoint.
    Modify as per your data source (e.g., API, database query).
    """
    try:
        response = requests.get(api_url)
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            print(f"Failed to fetch data. Status code: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {str(e)}")
        return None

def preprocess_data(data):
    """
    Function to preprocess the fetched data.
    Modify this function based on your specific preprocessing requirements.
    """
  
    df = pd.DataFrame(data)
    
    # Example: Convert timestamp strings to datetime objects
    df['timestamp'] = df['timestamp'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
    
    return df

def save_data(df, output_file):
    """
    Function to save ingested data to a file.
    Modify as per your output format (e.g., CSV, database).
    """
    df.to_csv(output_file, index=False)

def main():
    # API endpoint (adjust as necessary)
    api_url = 'https://api.example.com/data'
    
    # Output file path (adjust as necessary)
    output_file = 'raw_data.csv'
    
    # Fetch data
    print("Fetching data from API...")
    data = fetch_data(api_url)
    
    if data:
        # Preprocess data
        print("Preprocessing data...")
        processed_data = preprocess_data(data)
        
        # Save processed data
        print("Saving processed data...")
        save_data(processed_data, output_file)
        
        print("Data ingestion complete.")
    else:
        print("Data ingestion failed. Check logs for details.")

if __name__ == "__main__":
    main()
