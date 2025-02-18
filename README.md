# E-commerce Data Analytics Project

## Overview
This project demonstrates advanced data engineering and analytics capabilities using Python, DuckDB, and various data processing tools. It includes data generation, transformation, and analysis of e-commerce data with a focus on performance and scalability.

## Project Structure
```
.
├── data_generator.py        # Generates sample e-commerce data
├── parquet_converter.py     # Converts CSV to optimized Parquet
├── duckdb_setup.py         # Initializes DuckDB database
├── duckdb_views.py         # Creates analytical views
├── terminal_report.py      # Interactive terminal dashboard
├── ecommerce.duckdb        # DuckDB database file
├── complex_ecommerce_data.csv    # Raw data
└── ecommerce_analytics.parquet   # Optimized Parquet file
```

## Features
- Generation of complex, realistic e-commerce data
- Efficient data storage using Parquet format
- DuckDB integration for high-performance analytics
- Interactive terminal-based reporting
- Advanced SQL analytics with materialized views
- JSON and array data type handling
- Comprehensive data validation

## Data Schema
The dataset includes:
- Transaction details
- Customer information
- Product attributes (JSON)
- User behavior tracking
- Shipping information
- Price history
- Review data

## Installation

### Prerequisites
- Python 3.8+
- pip (Python package manager)

### Setup
1. Clone the repository:
```bash
git clone <repository-url>
cd e-commerce-analytics
```

2. Install required packages:
```bash
pip install pandas numpy faker duckdb pyarrow rich
```

3. Generate sample data:
```bash
python data_generator.py
```

4. Convert to Parquet:
```bash
python parquet_converter.py
```

5. Initialize DuckDB:
```bash
python duckdb_setup.py
```

6. Create analytical views:
```bash
python duckdb_views.py
```

7. Run the terminal dashboard:
```bash
python terminal_report.py
```

## Analytics Capabilities

### 1. Sales Analysis
- Daily/Monthly trends
- Category performance
- Revenue metrics
- Order statistics

### 2. Customer Analytics
- Customer segmentation
- Behavior analysis
- Purchase patterns
- Device preferences

### 3. Product Analytics
- Product performance
- Category insights
- Price trend analysis
- Review analytics

### 4. Shipping Analytics
- Carrier performance
- Delivery metrics
- Geographic distribution
- Shipping methods

## DuckDB Views

### daily_sales_metrics
- Daily aggregated sales data
- Revenue calculations
- Order counts
- Customer metrics

### product_performance
- Product-level analytics
- Sales metrics
- Rating analysis
- Price tracking

### customer_segments
- Customer categorization
- Purchase history
- Behavior patterns
- Lifetime value

### product_features_analysis
- Feature distribution
- Category correlations
- Attribute analysis

### shipping_analytics
- Shipping performance
- Carrier metrics
- Zone analysis

## Example Queries

### Basic Sales Analysis
```sql
SELECT * FROM daily_sales_metrics
ORDER BY sale_date DESC
LIMIT 5;
```

### Category Performance
```sql
SELECT 
    category_info_main,
    COUNT(*) as orders,
    SUM(quantity * base_price) as revenue
FROM ecommerce
GROUP BY category_info_main
ORDER BY revenue DESC;
```

### Customer Segmentation
```sql
SELECT 
    customer_segment,
    COUNT(*) as customer_count,
    AVG(total_spend) as avg_spend
FROM customer_segments
GROUP BY customer_segment;
```

## Performance Optimization
- Parquet compression for efficient storage
- DuckDB indices for faster queries
- Materialized views for common analytics
- Optimized data types and schemas




