# data_ingestion_demo.py

import pandas as pd

# Step 1: Load data
df = pd.read_csv(raw_data.csv')
print("Original Data:")
print(df.head())

# Step 2: Clean data
df = df.dropna()  # Remove rows with missing values
df['timestamp'] = pd.to_datetime(df['timestamp'])  # Convert to datetime

# Step 3: Save cleaned data
df.to_csv('data/cleaned_data.csv', index=False)
print("Cleaned data saved to 'data/cleaned_data.csv'")