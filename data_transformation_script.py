# data_transformation_script.py

import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler

def load_data(input_file):
    """
    Function to load data from a file.
    Modify as per your data source (e.g., CSV, database).
    """
    df = pd.read_csv(input_file)
    return df

def transform_data(df):
    """
    Function to perform data transformation.
    Modify this function based on your specific transformation requirements.
    """
   
    numerical_columns = df.select_dtypes(include=[np.number]).columns.tolist()
    scaler = StandardScaler()
    df[numerical_columns] = scaler.fit_transform(df[numerical_columns])
    
    
    df['transformed_column'] = df['existing_column'] * 2
    
    return df

def save_data(df, output_file):
    """
    Function to save transformed data to a file.
    Modify as per your output format (e.g., CSV, database).
    """
    df.to_csv(output_file, index=False)

def main():
    # Input and output file paths (adjust as necessary)
    input_file = 'input_data.csv'
    output_file = 'transformed_data.csv'
    
    # Load data
    print("Loading data...")
    data = load_data(input_file)
    
    # Perform transformation
    print("Transforming data...")
    transformed_data = transform_data(data)
    
    # Save transformed data
    print("Saving transformed data...")
    save_data(transformed_data, output_file)
    
    print("Data transformation complete.")

if __name__ == "__main__":
    main()
