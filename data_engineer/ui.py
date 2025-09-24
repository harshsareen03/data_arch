# ui.py — Orientation & Fundamentals demo pipeline
# Run with: python ui.py

import pandas as pd
import os
from datetime import datetime

# -----------------------------------------
# Section 1 — Setup
# -----------------------------------------
os.makedirs("raw", exist_ok=True)
os.makedirs("curated", exist_ok=True)

print("Environment ready. Folders created: raw/, curated/")

# -----------------------------------------
# Section 2 — Ingestion (Raw Zone)
# -----------------------------------------
data = {
    "id": [1, 2, 3],
    "name": ["alice", "bob", "carol"],
    "country": ["us", "uk", "de"]
    
}

df = pd.DataFrame(data)

# Save as CSV (source)
df.to_csv("data_sample.csv", index=False)

# # Read the CSV (simulate ingestion)
df_ingest = pd.read_csv("data_sample.csv")

print("Ingested rows:", len(df_ingest))
print(df_ingest.head())

# Save ingested file into Raw Zone as Parquet
raw_path = "raw/sample.parquet"
df_ingest.to_parquet(raw_path, index=False)

print(f"Raw snapshot saved at {raw_path}")

# -----------------------------------------
# Section 3 — Transformation (Curated Zone)
# -----------------------------------------
# Load from Raw Zone
df_raw = pd.read_parquet(raw_path)

# Add ingestion timestamp and normalize
df_raw["ingest_ts"] = datetime.now()
df_raw["country"] = df_raw["country"].str.upper()

# Save to Curated Zone
curated_path = "curated/sample_v1.parquet"
df_raw.to_parquet(curated_path, index=False)

print(f"Curated dataset saved at {curated_path}")
print(df_raw.head())


# Section 4 — Orchestration Placeholder
# -------------------------------------
# In real life: this code would be triggered by Airflow, Dagster, or a scheduler.
# For demo purposes, we simulate a simple "pipeline run" function.

# def pipeline_run():
#     print("Triggering pipeline: ingest → transform → curated")
#     df = pd.read_csv("data_sample.csv")
#     df.to_parquet("raw/demo.parquet", index=False)
#     df2 = pd.read_parquet("raw/demo.parquet")
#     df2["ingest_ts"] = datetime.now()
#     df2["country"] = df2["country"].str.upper()
#     df2.to_parquet("curated/demo.parquet", index=False)
#     print("Pipeline finished successfully.")

# pipeline_run()

# Section 5 — Monitoring / Checks Placeholder
# -------------------------------------------
# Example: simple row count & schema check

# df_curated = pd.read_parquet("curated/demo.parquet")

# print("Row count:", len(df_curated))
# print("Schema:")
# print(df_curated.dtypes)

# Basic assertion (quality check)
# assert "country" in df_curated.columns, "Missing 'country' column!"
# print("Quality check passed ✅")