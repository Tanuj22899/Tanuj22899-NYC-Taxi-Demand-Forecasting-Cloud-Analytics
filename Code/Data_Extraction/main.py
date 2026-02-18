import functions_framework
import pandas as pd
import requests
from google.cloud import storage
from io import BytesIO
import os

@functions_framework.http
def ingest_and_split_tlc_data(request):
    args = request.args
    year = args.get('year', '2024')
    month = args.get('month', '01')
    taxi_type = args.get('type', 'yellow').lower()
    BUCKET_NAME = "de2_gcs_ojas" 
    
    target_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month}.parquet"
    
    try:
        response = requests.get(target_url)
        response.raise_for_status()
        df = pd.read_parquet(BytesIO(response.content))
        
        # Normalize
        mapping = {'tpep_pickup_datetime': 'pickup_datetime', 'tpep_dropoff_datetime': 'dropoff_datetime'} if taxi_type == 'yellow' else {'lpep_pickup_datetime': 'pickup_datetime', 'lpep_dropoff_datetime': 'dropoff_datetime'}
        df['taxi_type'] = taxi_type
        df = df.rename(columns=mapping)
        df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])

        # Dynamic Split
        df = df.sort_values(by='pickup_datetime')
        weekly_groups = df.groupby(pd.Grouper(key='pickup_datetime', freq='W'))

        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)

        for i, (week_end, week_df) in enumerate(weekly_groups, 1):
            if not week_df.empty:
                week_df = week_df.drop(columns='Airport_fee', errors="ignore")
                week_start_str = week_df['pickup_datetime'].min().strftime('%Y-%m-%d')
                output_filename = f"{taxi_type}_W{i}_{week_start_str}.parquet"
                
                out_buffer = BytesIO()
                week_df.to_parquet(out_buffer, index=False)
                
                blob = bucket.blob(f"NewYork_Taxi/{output_filename}")
                blob.upload_from_string(out_buffer.getvalue(), content_type='application/octet-stream')
        
        return f"Successfully processed {taxi_type} {year}-{month}", 200
    except Exception as e:
        return f"Error: {str(e)}", 500