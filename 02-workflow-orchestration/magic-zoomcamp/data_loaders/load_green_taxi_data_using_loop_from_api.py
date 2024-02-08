import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    taxi_dtypes = {
        'VendorID' : pd.Int64Dtype(),
        'passenger_count' : pd.Int64Dtype(),
        'trip_distance' : float,
        'RatecodeID' : pd.Int64Dtype(),
        'store_and_fwd_flag' : str,
        'PULocationID' : pd.Int64Dtype(),
        'DOLocationID' : pd.Int64Dtype(),
        'payment_type' : pd.Int64Dtype(),
        'fare_amount' : float,
        'extra' : float,
        'mta_tax' : float,
        'tip_amount' : float,
        'tolls_amount' : float,
        'improvement_surcharge' : float,
        'total_amount' : float,
        'congestion_surcharge' : float,
    }
    parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']
    
    # Create a date range from 2020-10-01 to 2020-12-31 with monthly frequency
    daterange = pd.date_range(start='2020-10-01', end='2020-12-31', freq='M')
    
    # Initialize an empty list to store the dataframes
    dataframes = []
    
    # Loop over the date range
    for date in daterange:
        # Extract the year and month from the date
        #print(date)
        #print(date.month)
        year = date.year
        month = date.month
    
        # Construct the API end point url
        #url = f"http://myAPI/{year}-{month}"
        url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-{month}.csv.gz"
        
        # Load the data from the url into a dataframe
        df = pd.read_csv(url, sep=",", compression="gzip", dtype=taxi_dtypes, parse_dates=parse_dates)
        
        # Append the dataframe to the list
        dataframes.append(df)
    
    # Concatenate the dataframes into one
    data = pd.concat(dataframes)
    

    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
