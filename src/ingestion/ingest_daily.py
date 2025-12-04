import pandas as pd
import numpy as np
import random
from faker import Faker
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder.appName("DailyIngestion").get_databricks_support().getOrCreate()

# Initialize Faker
fake = Faker()

# --- CONFIGURATION ---
current_time = datetime.now()

# LOGIC FIX:
# We generate data for YESTERDAY to ensure the day is fully complete.
# If run on 2023-10-25 at 10:00 AM, we generate data for 2023-10-24.
business_date = current_time - timedelta(days=1)

# Processing time is NOW (when the ingestion job is actually running)
processing_time = current_time 

print(f"Job running at: {processing_time}")
print(f"Generating data for business date: {business_date.strftime('%Y-%m-%d')}")

def generate_daily_batch():
    
    # 1. GENERATE NEW TRANSACTIONS
    data_tx = []
    
    # Customer Pool Logic (10k pool, 1.5k active)
    all_possible_ids = range(1, 10001)
    daily_active_users = random.sample(all_possible_ids, 1500)
    
    for _ in range(5000):
        # A. Randomize Time Logic
        # We use 'business_date' (Yesterday), so we can safely use ANY time (00:00 to 23:59)
        random_hour = random.randint(0, 23)
        random_minute = random.randint(0, 59)
        random_second = random.randint(0, 59)
        
        txn_datetime = business_date.replace(
            hour=random_hour, 
            minute=random_minute, 
            second=random_second, 
            microsecond=0
        )
        
        # LOGIC CHECK: 
        # Since txn_datetime is yesterday and processing_time is today,
        # txn_datetime < processing_time is ALWAYS true.

        # B. Select Customer
        cust_num = random.choice(daily_active_users)
        customer_id = f'CUST_{str(cust_num).zfill(5)}'

        # C. Select Type
        txn_type = random.choice(['Purchase', 'Withdrawal', 'Transfer', 'Deposit'])
        
        # D. Logic for Amount, Category, Channel
        purchase_category = None
        purchase_channel = None 

        if txn_type == 'Purchase':
            amount = round(random.uniform(5, 400), 2) * -1
            purchase_category = random.choice(['Food', 'Tech', 'Travel', 'Retail', 'Entertainment'])
            purchase_channel = random.choice(['Online', 'In-Store'])
        elif txn_type == 'Deposit':
            amount = round(random.uniform(1000, 5000), 2)
        else:
            amount = round(random.uniform(20, 500), 2) * -1

        data_tx.append({
            'transaction_id': fake.uuid4(),
            'customer_id': customer_id,
            'transaction_date': txn_datetime,   # e.g., Yesterday 14:00
            'time_processed': processing_time,  # e.g., Today 10:00
            'amount': amount,
            'currency': 'USD',
            'transaction_type': txn_type,
            'purchase_category': purchase_category,
            'purchase_channel': purchase_channel,
            'status': 'Completed'
        })
        
    df_tx_pandas = pd.DataFrame(data_tx)
    
    # 2. GENERATE FX RATES
    data_fx = []
    eur_rate = 1.10 + np.random.normal(0, 0.005)
    gbp_rate = 1.25 + np.random.normal(0, 0.005)
    
    data_fx.append({
        'date': business_date.date(), # Rate for YESTERDAY
        'currency': 'EUR', 
        'rate_to_usd': eur_rate,
        'time_processed': processing_time
    })
    data_fx.append({
        'date': business_date.date(), 
        'currency': 'GBP', 
        'rate_to_usd': gbp_rate,
        'time_processed': processing_time
    })
    
    df_fx_pandas = pd.DataFrame(data_fx)

    return df_tx_pandas, df_fx_pandas

# --- EXECUTION ---

if __name__ == "__main__":
    pdf_transactions, pdf_rates = generate_daily_batch()

    sdf_transactions = spark.createDataFrame(pdf_transactions)
    sdf_rates = spark.createDataFrame(pdf_rates)

    bronze_tx_table = "fintech_ops.bronze.transactions_history"
    bronze_fx_table = "fintech_ops.bronze.exchange_rates_history"

    print(f"Writing to {bronze_tx_table}...")
    sdf_transactions.write.format("delta").mode("append").saveAsTable(bronze_tx_table)

    print(f"Writing to {bronze_fx_table}...")
    sdf_rates.write.format("delta").mode("append").saveAsTable(bronze_fx_table)

    print("Success! Data successfully appended.")