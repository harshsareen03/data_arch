import pandas as pd

df=pd.read_csv('raw_data.csv')

print("orginal data:")

print(df.head())



df=df.dropna()
df['timestamp']=pd.to_datetime(df['timestamp'])

df.to_csv('data/cleaned_data.csv',index=False)
print("cleaned data saved to 'data/cleaned_data.csv'")