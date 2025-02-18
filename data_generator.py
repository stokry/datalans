import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import random
import uuid
from faker import Faker

fake = Faker()

# Set random seed for reproducibility
np.random.seed(42)
random.seed(42)

def generate_product_attributes():
    """Generate random product attributes in JSON format"""
    attributes = {
        'size': random.choice(['S', 'M', 'L', 'XL', 'XXL', None]),
        'color': random.choice(['Red', 'Blue', 'Green', 'Black', 'White', None]),
        'material': random.choice(['Cotton', 'Polyester', 'Wool', 'Silk', 'Leather', None]),
        'features': random.sample(['Waterproof', 'Breathable', 'UV Protection', 'Quick Dry', 'Stain Resistant'], 
                                random.randint(0, 3)),
        'warranty_months': random.choice([12, 24, 36, None])
    }
    return json.dumps(attributes)

def generate_user_behavior():
    """Generate complex user behavior data"""
    actions = []
    for _ in range(random.randint(1, 5)):
        action = {
            'timestamp': (datetime.now() - timedelta(days=random.randint(1, 30))).isoformat(),
            'action_type': random.choice(['view', 'cart_add', 'wishlist_add', 'purchase']),
            'device': random.choice(['mobile', 'desktop', 'tablet']),
            'session_duration': random.randint(30, 3600),
            'page_views': random.randint(1, 20)
        }
        actions.append(action)
    return json.dumps(actions)

def generate_shipping_info():
    """Generate complex shipping information"""
    shipping = {
        'carrier': random.choice(['FedEx', 'UPS', 'DHL', 'USPS']),
        'method': random.choice(['Standard', 'Express', 'Next Day', 'International']),
        'tracking_number': str(uuid.uuid4()),
        'estimated_delivery': (datetime.now() + timedelta(days=random.randint(1, 14))).isoformat(),
        'shipping_zones': random.sample(['NA', 'EU', 'ASIA', 'AU'], random.randint(1, 3)),
        'restrictions': random.sample(['Hazmat', 'Oversized', 'Fragile', 'Perishable'], random.randint(0, 2))
    }
    return json.dumps(shipping)

def generate_nested_categories():
    """Generate nested product categories"""
    main_categories = ['Electronics', 'Fashion', 'Home', 'Sports']
    sub_categories = {
        'Electronics': ['Smartphones', 'Laptops', 'Accessories', 'Gaming'],
        'Fashion': ['Clothing', 'Shoes', 'Accessories', 'Watches'],
        'Home': ['Furniture', 'Decor', 'Kitchen', 'Garden'],
        'Sports': ['Equipment', 'Clothing', 'Shoes', 'Accessories']
    }
    
    main_cat = random.choice(main_categories)
    sub_cat = random.choice(sub_categories[main_cat])
    return json.dumps({'main': main_cat, 'sub': sub_cat})

def generate_price_history():
    """Generate price history with promotions"""
    num_changes = random.randint(2, 5)
    base_price = random.uniform(10, 1000)
    history = []
    
    for i in range(num_changes):
        change = {
            'date': (datetime.now() - timedelta(days=30-i*7)).isoformat(),
            'price': round(base_price * random.uniform(0.8, 1.2), 2),
            'promotion_type': random.choice(['None', 'Holiday Sale', 'Clearance', 'Flash Sale', None]),
            'discount_percentage': random.choice([0, 10, 15, 20, 25, 30])
        }
        history.append(change)
    return json.dumps(history)

# Generate the dataset
data = []
for _ in range(10000):
    record = {
        'transaction_id': str(uuid.uuid4()),
        'timestamp': fake.date_time_between(start_date='-1y', end_date='now').isoformat(),
        'customer_id': str(uuid.uuid4()),
        'product_id': str(uuid.uuid4()),
        'quantity': random.randint(1, 5),
        'base_price': round(random.uniform(10, 1000), 2),
        'currency': random.choice(['USD', 'EUR', 'GBP', 'JPY', 'AUD']),
        'payment_method': random.choice(['credit_card', 'paypal', 'crypto', 'bank_transfer']),
        'status': random.choice(['completed', 'pending', 'failed', 'refunded']),
        'product_attributes': generate_product_attributes(),
        'user_behavior': generate_user_behavior(),
        'shipping_info': generate_shipping_info(),
        'category_info': generate_nested_categories(),
        'price_history': generate_price_history(),
        'customer_notes': fake.text() if random.random() < 0.3 else None,
        'review_score': random.choice([1, 2, 3, 4, 5, None]),
        'review_text': fake.text() if random.random() < 0.2 else None,
        'is_gift': random.choice([True, False]),
        'gift_message': fake.text() if random.random() < 0.1 else None,
        'return_reason': random.choice(['size_issue', 'quality_issue', 'wrong_item', None]),
        'marketing_source': random.choice(['organic_search', 'paid_search', 'social_media', 'email', None]),
        'session_id': str(uuid.uuid4()),
        'ip_address': fake.ipv4(),
        'user_agent': fake.user_agent()
    }
    data.append(record)

# Convert to DataFrame and save to CSV
df = pd.DataFrame(data)
df.to_csv('complex_ecommerce_data.csv', index=False)

# Print sample of the data and basic information
print(f"Dataset shape: {df.shape}")
print("\nSample record:")
print(json.dumps(data[0], indent=2))
print("\nColumns with JSON content:")
for col in ['product_attributes', 'user_behavior', 'shipping_info', 'category_info', 'price_history']:
    print(f"\n{col} example:")
    print(json.loads(df[col].iloc[0]))