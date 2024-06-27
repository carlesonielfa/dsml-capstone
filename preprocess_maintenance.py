import pandas as pd
from tqdm import tqdm
import concurrent.futures
import logging
import os

subfolder = 'data/'
files = os.listdir(subfolder)
files = [f for f in files if f.endswith('.csv')]

def exclusive_mode(x):
    mode = x.mode(dropna=True)
    if mode is None or len(mode) == 0:
        return "IN_SERVICE"
    return mode.iloc[0]

def clean_df(df: pd.DataFrame):
    # Convert Unix timestamp to datetime
    df['last_reported'] = pd.to_datetime(df['last_reported'], unit='s')
    df['last_updated'] = pd.to_datetime(df['last_updated'], unit='s')

    # Keep only the necessary columns
    cols_to_keep = ['station_id', 'last_reported', 'num_bikes_available', 'num_bikes_available_types.mechanical', 
                    'num_bikes_available_types.ebike', 'num_docks_available', "status", 
                    "is_renting", "is_returning", "is_installed"]
    df = df[cols_to_keep].copy()

    # Extract hour, day, month, year from last_reported
    df['hour'] = df['last_reported'].dt.hour
    df['day'] = df['last_reported'].dt.day
    df['month'] = df['last_reported'].dt.month
    df['year'] = df['last_reported'].dt.year

    # Remove rows with year before 2019 or after 2024
    df = df[(df['year'] >= 2019) & (df['year'] <= 2024)]

    # Drop last_reported column
    df.drop(columns=['last_reported'], inplace=True)

    # Convert station_id to int
    df['station_id'] = df['station_id'].astype(int)

    # Set IN_SERVICE to NaN so that it isn't counted as a mode
    df['status'] = df['status'].replace('IN_SERVICE', pd.NA)

    # Define the aggregation functions
    agg_funcs = {
        'num_bikes_available': 'mean',
        'num_bikes_available_types.mechanical': 'mean',
        'num_bikes_available_types.ebike': 'mean',
        'num_docks_available': 'mean',
        'status': exclusive_mode,
        'is_renting': lambda x: x.mode().iloc[0],
        'is_returning': lambda x: x.mode().iloc[0],
        'is_installed': lambda x: x.mode().iloc[0]
    }

    # Aggregate by station_id, hour, day, month, year
    df = df.groupby(['station_id', 'hour', 'day', 'month', 'year']).agg(agg_funcs).reset_index()

    return df

# Function to process each file
def process_file(file):
    try:
        df = pd.read_csv(subfolder + file)
        df = clean_df(df)
        logging.info(f"Processed file {file}")
        return df
    except Exception as e:
        logging.error(f"Error processing file {file}: {e}")
        raise e

if __name__ == "__main__":
    # Set the maximum number of worker processes
    max_workers = 24  # Adjust this number as needed

    # Use ProcessPoolExecutor for parallel processing
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Map the process_file function to the list of files
        results = list(tqdm(executor.map(process_file, files[:-1]), total=len(files[:-1])))

    # Concatenate all the results into a single DataFrame
    big_df = pd.concat(results)

    # Save the concatenated DataFrame to a Parquet file
    big_df.to_parquet('data/cleaned_maintenance_data.parquet', index=False)
