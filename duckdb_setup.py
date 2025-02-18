import duckdb
import os

def initialize_duckdb():
    # Create a connection to a new or existing DuckDB database
    print("Initializing DuckDB database...")
    conn = duckdb.connect('ecommerce.duckdb')
    
    try:
        # Check if Parquet file exists
        if not os.path.exists('ecommerce_analytics.parquet'):
            raise FileNotFoundError("Parquet file 'ecommerce_analytics.parquet' not found!")

        # Create the ecommerce table from Parquet file
        print("Creating ecommerce table from Parquet file...")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ecommerce AS 
            SELECT * FROM read_parquet('ecommerce_analytics.parquet');
        """)
        
        # Create indices for common query patterns
        print("Creating indices for optimization...")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_customer ON ecommerce(customer_id);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON ecommerce(timestamp);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_category ON ecommerce(category_info_main, category_info_sub);")
        
        # Verify the data
        result = conn.execute("SELECT COUNT(*) as total_rows FROM ecommerce").fetchone()
        print(f"\nTotal rows in database: {result[0]}")
        
        # Show table schema
        print("\nTable Schema:")
        schema = conn.execute("DESCRIBE ecommerce").fetchall()
        for column in schema:
            print(f"{column[0]}: {column[1]}")
        
        # Basic data validation
        print("\nBasic data validation:")
        validation = conn.execute("""
            SELECT 
                COUNT(DISTINCT customer_id) as unique_customers,
                COUNT(DISTINCT product_id) as unique_products,
                COUNT(DISTINCT category_info_main) as main_categories,
                MIN(timestamp) as earliest_date,
                MAX(timestamp) as latest_date
            FROM ecommerce
        """).fetchone()
        
        print(f"Unique Customers: {validation[0]}")
        print(f"Unique Products: {validation[1]}")
        print(f"Main Categories: {validation[2]}")
        print(f"Date Range: {validation[3]} to {validation[4]}")
        
        print("\nDuckDB database initialized successfully!")
        return conn
        
    except Exception as e:
        print(f"Error during initialization: {str(e)}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    initialize_duckdb()