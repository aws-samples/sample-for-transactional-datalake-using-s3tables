import boto3
import random
import time
from datetime import datetime, timedelta
from decimal import Decimal
import uuid


def create_sample_order():
    # Product categories and their price ranges
    categories = {
        "Electronics": (299.99, 1299.99),
        "Accessories": (9.99, 99.99),
        "Books": (4.99, 49.99),
        "Clothing": (19.99, 199.99),
        "Home": (29.99, 499.99),
    }

    # Regions and payment methods
    regions = ["East", "West", "North", "South", "Central"]
    payment_methods = ["Credit Card", "Debit Card", "PayPal", "Bank Transfer"]

    # Generate random timestamp within last 90 days
    end_date = datetime.now()
    start_date = end_date - timedelta(days=90)
    random_timestamp = random.uniform(start_date.timestamp(), end_date.timestamp())

    # Generate 1-5 items per order
    num_items = random.randint(1, 5)
    items = []
    total_amount = Decimal("0.0")

    for _ in range(num_items):
        category = random.choice(list(categories.keys()))
        min_price, max_price = categories[category]
        price = round(Decimal(str(random.uniform(min_price, max_price))), 2)
        quantity = random.randint(1, 5)

        item = {
            "productId": f"PROD{uuid.uuid4().hex[:8].upper()}",
            "category": category,
            "price": price,
            "quantity": quantity,
        }
        items.append(item)
        total_amount += price * Decimal(str(quantity))

    order = {
        "orderId": f"ORD{uuid.uuid4().hex[:8].upper()}",
        "timestamp": int(random_timestamp * 1000),  # Convert to milliseconds
        "customerId": f"CUST{uuid.uuid4().hex[:8].upper()}",
        "items": items,
        "totalAmount": round(total_amount, 2),
        "region": random.choice(regions),
        "paymentMethod": random.choice(payment_methods),
    }

    return order


def batch_write_orders(table_name, num_orders=1000):
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)

    # Process in batches of 25 (DynamoDB batch write limit)
    batch_size = 25
    orders_processed = 0

    print(f"Starting to write {num_orders} orders to DynamoDB...")
    start_time = time.time()

    try:
        with table.batch_writer() as batch:
            for _ in range(num_orders):
                order = create_sample_order()
                batch.put_item(Item=order)
                orders_processed += 1

                # Progress update every 100 orders
                if orders_processed % 100 == 0:
                    print(f"Processed {orders_processed} orders...")

    except Exception as e:
        print(f"Error writing to DynamoDB: {str(e)}")
        return False

    end_time = time.time()
    duration = end_time - start_time
    print(f"\nCompleted writing {orders_processed} orders in {duration:.2f} seconds")
    print(f"Average time per order: {(duration/orders_processed):.3f} seconds")

    return True


def create_orders_table(table_name):
    dynamodb = boto3.resource("dynamodb")

    try:
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {"AttributeName": "orderId", "KeyType": "HASH"},
                {"AttributeName": "timestamp", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "orderId", "AttributeType": "S"},
                {"AttributeName": "timestamp", "AttributeType": "N"},
            ],
            ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
        )

        # Wait for the table to be created
        table.meta.client.get_waiter("table_exists").wait(TableName=table_name)
        print(f"Table {table_name} created successfully")
        return True

    except dynamodb.meta.client.exceptions.ResourceInUseException:
        print(f"Table {table_name} already exists")
        return True
    except Exception as e:
        print(f"Error creating table: {str(e)}")
        return False


def main():
    table_name = "Orders"

    # Create table if it doesn't exist
    if create_orders_table(table_name):
        # Write sample orders
        batch_write_orders(table_name)
    else:
        print("Failed to create/verify table. Exiting...")


if __name__ == "__main__":
    main()
