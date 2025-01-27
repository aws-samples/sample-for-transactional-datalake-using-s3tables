import boto3
import random
import time
from datetime import datetime, timedelta
from decimal import Decimal
import uuid


class CreateSampleData:
    def __init__(self):
        # Initialize DynamoDB client
        self.dynamodb = boto3.resource("dynamodb")
        self.table_name = "financial-transactions"
        self.table = self.dynamodb.Table(self.table_name)

    @staticmethod
    def generate_transaction(timestamp, customer_base=1000):
        merchant_categories = [
            "RETAIL",
            "DINING",
            "TRAVEL",
            "ENTERTAINMENT",
            "HEALTHCARE",
            "UTILITIES",
            "FINANCIAL_SERVICES",
        ]

        payment_methods = [
            "CREDIT_CARD",
            "DEBIT_CARD",
            "DIGITAL_WALLET",
            "BANK_TRANSFER",
        ]

        transaction_types = ["PURCHASE", "REFUND", "PAYMENT", "TRANSFER", "WITHDRAWAL"]

        currencies = ["USD", "EUR", "GBP", "INR"]

        risk_scores = ["LOW", "MEDIUM", "HIGH"]

        regions = ["US_EAST", "US_WEST", "EU", "APAC"]

        return {
            "transaction_id": f"TXN_{uuid.uuid4().hex[:16]}",
            "timestamp": int(timestamp.timestamp() * 1000),
            "customer_id": f"CUST_{str(random.randint(1, customer_base)).zfill(6)}",
            "date": timestamp.strftime("%Y-%m-%d"),
            "hour": int(timestamp.hour),
            "minute": int(timestamp.minute),
            "transaction_type": random.choice(transaction_types),
            "amount": Decimal(str(round(random.uniform(10, 1000), 2))),
            "currency": random.choice(currencies),
            "merchant_category": random.choice(merchant_categories),
            "payment_method": random.choice(payment_methods),
            "region": random.choice(regions),
            "risk_score": random.choice(risk_scores),
            "transaction_metadata": {
                "device_type": random.choice(["MOBILE", "WEB", "POS", "ATM"]),
                "authentication_method": random.choice(
                    ["2FA", "BIOMETRIC", "PIN", "PASSWORD"]
                ),
                "merchant_id": f"MERCH_{random.randint(1, 1000):04d}",
            },
            "fraud_indicators": {
                "velocity_check": random.choice(["PASS", "FLAG", "REVIEW"]),
                "amount_threshold": random.choice(["NORMAL", "HIGH", "VERY_HIGH"]),
                "location_risk": random.choice(["LOW", "MEDIUM", "HIGH"]),
                "pattern_match": random.choice(["NORMAL", "SUSPICIOUS"]),
            },
            "status": random.choice(
                ["APPROVED", "DECLINED", "PENDING_REVIEW", "FLAGGED"]
            ),
            "processing_timestamp": int(
                (
                    timestamp + timedelta(milliseconds=random.randint(100, 1000))
                ).timestamp()
                * 1000
            ),
        }

    def batch_write_transactions(self, num_transactions=100):
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=1)  # Generate last hour of data

        # Generate random timestamps within the last hour
        timestamps = [
            start_time + timedelta(seconds=random.randint(0, 3600))
            for _ in range(num_transactions)
        ]
        timestamps.sort()  # Sort timestamps for more realistic data

        try:
            with self.table.batch_writer() as batch:
                for timestamp in timestamps:
                    transaction = self.generate_transaction(timestamp)
                    batch.put_item(Item=transaction)

            print(f"Successfully wrote {num_transactions} transactions")

        except Exception as e:
            print(f"Error writing to DynamoDB: {str(e)}")
            raise


def main():
    sample = CreateSampleData()

    sample.batch_write_transactions()


if __name__ == "__main__":
    main()
