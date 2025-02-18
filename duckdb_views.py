import duckdb

def create_analytical_views():
    # Connect to the database
    conn = duckdb.connect('ecommerce.duckdb')
    
    try:
        # 1. Create view for daily sales metrics
        print("Creating daily_sales_metrics view...")
        conn.execute("""
        CREATE OR REPLACE VIEW daily_sales_metrics AS
        SELECT 
            DATE_TRUNC('day', timestamp) as sale_date,
            COUNT(*) as total_transactions,
            COUNT(DISTINCT customer_id) as unique_customers,
            SUM(quantity * base_price) as total_revenue,
            AVG(quantity * base_price) as avg_order_value,
            SUM(CASE WHEN is_gift THEN 1 ELSE 0 END) as gift_orders,
            SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END)::FLOAT / COUNT(*) as completion_rate
        FROM ecommerce
        GROUP BY 1
        ORDER BY 1;
        """)

        # 2. Create view for product performance
        print("Creating product_performance view...")
        conn.execute("""
        CREATE OR REPLACE VIEW product_performance AS
        SELECT 
            product_id,
            category_info_main,
            category_info_sub,
            COUNT(*) as total_sales,
            SUM(quantity) as units_sold,
            AVG(base_price) as avg_price,
            AVG(review_score) as avg_rating,
            COUNT(CASE WHEN review_score IS NOT NULL THEN 1 END) as review_count,
            price_history_avg_price as historical_avg_price,
            price_history_max_discount as max_discount_offered
        FROM ecommerce
        GROUP BY 
            product_id, 
            category_info_main, 
            category_info_sub,
            price_history_avg_price,
            price_history_max_discount;
        """)

        # 3. Create view for customer segmentation
        print("Creating customer_segments view...")
        conn.execute("""
        CREATE OR REPLACE VIEW customer_segments AS
        WITH customer_metrics AS (
            SELECT 
                customer_id,
                COUNT(*) as purchase_count,
                SUM(quantity * base_price) as total_spend,
                AVG(quantity * base_price) as avg_order_value,
                MAX(timestamp) as last_purchase,
                MIN(timestamp) as first_purchase,
                COUNT(DISTINCT DATE_TRUNC('month', timestamp)) as active_months,
                MODE(user_behavior_primary_device) as preferred_device
            FROM ecommerce
            GROUP BY customer_id
        )
        SELECT 
            *,
            CASE 
                WHEN purchase_count >= 3 AND total_spend >= 500 THEN 'VIP'
                WHEN purchase_count >= 2 OR total_spend >= 250 THEN 'Regular'
                ELSE 'New'
            END as customer_segment,
            DATE_DIFF('day', first_purchase, last_purchase) as customer_lifetime_days
        FROM customer_metrics;
        """)

        # 4. Create view for product features analysis
        print("Creating product_features_analysis view...")
        conn.execute("""
        CREATE OR REPLACE VIEW product_features_analysis AS
        WITH RECURSIVE unnested_features AS (
            SELECT 
                product_id,
                category_info_main,
                category_info_sub,
                UNNEST(product_attributes_features) as feature
            FROM ecommerce
            WHERE product_attributes_features IS NOT NULL
        )
        SELECT 
            category_info_main,
            category_info_sub,
            feature,
            COUNT(*) as feature_count,
            COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY category_info_main) as feature_percentage
        FROM unnested_features
        GROUP BY 1, 2, 3;
        """)

        # 5. Create view for shipping analytics
        print("Creating shipping_analytics view...")
        conn.execute("""
        CREATE OR REPLACE VIEW shipping_analytics AS
        WITH shipping_zones_unnested AS (
            SELECT 
                shipping_info_carrier,
                shipping_info_method,
                UNNEST(shipping_info_shipping_zones) as shipping_zone,
                UNNEST(shipping_info_restrictions) as restriction,
                status,
                timestamp
            FROM ecommerce
        )
        SELECT 
            shipping_info_carrier,
            shipping_info_method,
            shipping_zone,
            restriction,
            COUNT(*) as shipment_count,
            COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_shipments,
            AVG(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completion_rate
        FROM shipping_zones_unnested
        GROUP BY 1, 2, 3, 4;
        """)

        # Verify views were created
        print("\nVerifying created views:")
        views = conn.execute("""
            SELECT view_name 
            FROM duckdb_views() 
            WHERE view_name IN (
                'daily_sales_metrics',
                'product_performance',
                'customer_segments',
                'product_features_analysis',
                'shipping_analytics'
            );
        """).fetchall()
        
        print("Successfully created views:")
        for view in views:
            print(f"- {view[0]}")

        # Example queries using the views
        print("\nSample queries you can run:")
        print("""
        -- Daily sales trend
        SELECT * FROM daily_sales_metrics ORDER BY sale_date DESC LIMIT 5;

        -- Top performing products
        SELECT * FROM product_performance 
        ORDER BY total_sales DESC LIMIT 5;

        -- Customer segment distribution
        SELECT customer_segment, COUNT(*) as count 
        FROM customer_segments 
        GROUP BY 1;

        -- Popular features by category
        SELECT * FROM product_features_analysis 
        WHERE feature_count > 10 
        ORDER BY feature_count DESC;

        -- Shipping carrier performance
        SELECT * FROM shipping_analytics 
        ORDER BY shipment_count DESC LIMIT 5;
        """)

    except Exception as e:
        print(f"Error creating views: {str(e)}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    create_analytical_views()