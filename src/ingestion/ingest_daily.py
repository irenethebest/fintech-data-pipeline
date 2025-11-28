import pandas as pd
import numpy as np
import random
from faker import Faker
from datetime import datetime
from pyspark.sql.functions import lit

# Initialize Faker
fake = Faker()
# We remove the static seed here. 
# Since this runs daily, we want DIFFERENT data every day.

# --- CONFIGURATION ---
# In a real pipeline, you often parameterize the date
execution_date = datetime.now()
print(f"Generating data for batch date: {execution_date.strftime('%Y-%m-%d')}")

def generate_daily_batch():
    
    # 1. GENERATE NEW TRANSACTIONS (Fact Table)
    # ------------------------------------------------
    # Simulating 2000 transactions happening "today"
    data_tx = []
    
    # Existing customer base IDs (assuming 50 existing customers)
    # In a real scenario, you'd read existing IDs from the Customer table
    customer_ids = [f'CUST_{str(i).zfill(4)}' for i in range(1, 51)]
    
    for _ in range(2000):
        txn_type = random.choice(['Purchase', 'Withdrawal', 'Transfer', 'Deposit'])
        
        # Logic to determine amount (simplified for brevity)
        if txn_type == 'Deposit':
            amount = round(random.uniform(1000, 5000), 2)
            category = 'Salary'
        else:
            amount = round(random.uniform(5, 400), 2) * -1
            category = random.choice(['Food', 'Tech', 'Travel'])

        data_tx.append({
            'transaction_id': fake.uuid4(),
            'customer_id': random.choice(customer_ids),
            'transaction_date': execution_date, # STRICTLY TODAY
            'amount': amount,
            'currency': 'USD',
            'transaction_type': txn_type,
            'category': category,
            'status': 'Completed'
        })
        
    df_tx_pandas = pd.DataFrame(data_tx)
    
    # 2. GENERATE DAILY EXCHANGE RATES (Reference Table)
    # ------------------------------------------------
    # Just generating one row per currency for "today"
    data_fx = []
    # Add random fluctuation to a base rate
    eur_rate = 1.10 + np.random.normal(0, 0.005)
    gbp_rate = 1.25 + np.random.normal(0, 0.005)
    
    data_fx.append({'date': execution_date.date(), 'currency': 'EUR', 'rate_to_usd': eur_rate})
    data_fx.append({'date': execution_date.date(), 'currency': 'GBP', 'rate_to_usd': gbp_rate})
    
    df_fx_pandas = pd.DataFrame(data_fx)

    return df_tx_pandas, df_fx_pandas

# --- EXECUTION & WRITE TO DELTA ---

# Generate the data in Pandas
pdf_transactions, pdf_rates = generate_daily_batch()

# Convert to Spark DataFrames
sdf_transactions = spark.createDataFrame(pdf_transactions)
sdf_rates = spark.createDataFrame(pdf_rates)

# Write to Delta Lake (Bronze Layer)
# Mode 'append' adds new rows without deleting old ones
table_path_tx = "/mnt/delta/finance/transactions_bronze" 
table_path_fx = "/mnt/delta/finance/exchange_rates_bronze"

# Note: In Unity Catalog, you would use saveAsTable("catalog.schema.table")
# Here we use saveAsTable with a default path or managed table approach
sdf_transactions.write.format("delta").mode("append").saveAsTable("finance_transactions_bronze")
sdf_rates.write.format("delta").mode("append").saveAsTable("finance_exchange_rates_bronze")

print(f"Success! Appended 2000 transactions and 2 FX rates for {execution_date}")