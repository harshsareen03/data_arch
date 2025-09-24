import great_expectations as ge
from sqlalchemy import create_engine
import pandas as pd

# ------------------------------
# Connect to Postgres
# ------------------------------
engine = create_engine("postgresql://de_user:de_pass@localhost:5432/de_db")

# Load data into a Pandas DataFrame first
df_pd = pd.read_sql("SELECT * FROM events", engine)

# Wrap with Great Expectations
df = ge.from_pandas(df_pd)

# ------------------------------
# Define expectations
# ------------------------------

# 1️⃣ user_id should not be null
df.expect_column_values_to_not_be_null("user_id")

# 2️⃣ action should be one of allowed set
df.expect_column_values_to_be_in_set("action", ["view", "purchase", "cart"])

# 3️⃣ processed_at should not be null
df.expect_column_values_to_not_be_null("processed_at")

# ------------------------------
# Validate
# ------------------------------
result = df.validate()

print(result)
