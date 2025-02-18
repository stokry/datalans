import pandas as pd
import json
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
import os

def normalize_json_column(df, column_name):
    """
    Normalize a JSON string column into separate columns
    """
    try:
        # Parse JSON strings to dictionaries
        parsed = df[column_name].apply(json.loads)
        
        # Convert to DataFrame and prefix column names
        if len(parsed) > 0:
            normalized = pd.json_normalize(parsed)
            normalized.columns = [f"{column_name}_{col}" for col in normalized.columns]
            
            # Drop the original JSON column
            df = df.drop(columns=[column_name])
            
            # Join the normalized columns back to the original dataframe
            df = pd.concat([df, normalized], axis=1)
        
        return df
    except Exception as e:
        print(f"Error normalizing column {column_name}: {str(e)}")
        return df

def process_user_behavior(df):
    """
    Special handling for user_behavior array column
    """
    def extract_behavior_metrics(behavior_json):
        try:
            behaviors = json.loads(behavior_json)
            return {
                'total_actions': len(behaviors),
                'total_duration': sum(b['session_duration'] for b in behaviors),
                'total_page_views': sum(b['page_views'] for b in behaviors),
                'last_action': behaviors[-1]['action_type'] if behaviors else None,
                'primary_device': max([b['device'] for b in behaviors],
                                    key=lambda x: [b['device'] for b in behaviors].count(x))
            }
        except Exception:
            return {
                'total_actions': 0,
                'total_duration': 0,
                'total_page_views': 0,
                'last_action': None,
                'primary_device': None
            }
    
    behavior_metrics = df['user_behavior'].apply(extract_behavior_metrics)
    behavior_df = pd.DataFrame(behavior_metrics.tolist())
    behavior_df.columns = [f'user_behavior_{col}' for col in behavior_df.columns]
    
    df = df.drop(columns=['user_behavior'])
    df = pd.concat([df, behavior_df], axis=1)
    return df

def process_price_history(df):
    """
    Extract key metrics from price history
    """
    def extract_price_metrics(history_json):
        try:
            history = json.loads(history_json)
            prices = [h['price'] for h in history]
            return {
                'price_changes_count': len(history),
                'max_price': max(prices),
                'min_price': min(prices),
                'avg_price': sum(prices) / len(prices),
                'max_discount': max([h['discount_percentage'] for h in history]),
                'last_promotion_type': history[-1]['promotion_type'] if history else None
            }
        except Exception:
            return {
                'price_changes_count': 0,
                'max_price': 0,
                'min_price': 0,
                'avg_price': 0,
                'max_discount': 0,
                'last_promotion_type': None
            }
    
    price_metrics = df['price_history'].apply(extract_price_metrics)
    price_df = pd.DataFrame(price_metrics.tolist())
    price_df.columns = [f'price_history_{col}' for col in price_df.columns]
    
    df = df.drop(columns=['price_history'])
    df = pd.concat([df, price_df], axis=1)
    return df

def convert_to_parquet():
    try:
        # Read the CSV file
        print("Reading CSV file...")
        df = pd.read_csv('complex_ecommerce_data.csv')
        
        # Convert timestamp strings to datetime
        print("Converting timestamps...")
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Process JSON columns
        json_columns = ['product_attributes', 'shipping_info', 'category_info']
        
        print("Normalizing JSON columns...")
        for col in json_columns:
            df = normalize_json_column(df, col)
        
        # Special handling for array-like JSON columns
        print("Processing user behavior data...")
        df = process_user_behavior(df)
        
        print("Processing price history data...")
        df = process_price_history(df)
        
        # Convert to PyArrow Table with optimized schema
        print("Creating optimized schema...")
        table = pa.Table.from_pandas(df)
        
        # Write to Parquet with optimizations
        print("Writing to Parquet...")
        pq.write_table(
            table,
            'ecommerce_analytics.parquet',
            compression='snappy',        # Good balance of compression and speed
            row_group_size=100000,      # Optimized for analytical queries
            use_dictionary=True,        # Enable dictionary encoding
            data_page_size=1048576     # 1MB pages
        )
        
        # Print schema information
        print("\nParquet Schema:")
        print(table.schema)
        
        # Print basic statistics
        print(f"\nTotal rows: {len(df)}")
        print(f"Total columns: {len(df.columns)}")
        print("\nMemory usage before and after:")
        csv_size = df.memory_usage(deep=True).sum() / 1024**2
        parquet_size = os.path.getsize('ecommerce_analytics.parquet') / 1024**2
        print(f"CSV memory usage: {csv_size:.2f} MB")
        print(f"Parquet file size: {parquet_size:.2f} MB")
        print(f"Compression ratio: {csv_size/parquet_size:.2f}x")
        
        print("\nParquet file successfully created!")
        
    except Exception as e:
        print(f"Error during conversion: {str(e)}")
        raise

if __name__ == "__main__":
    convert_to_parquet()