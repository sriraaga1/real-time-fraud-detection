import pandas as pd

df = pd.read_parquet("output/fraud_results")

df.to_csv("output/fraud_results.csv", index=False)

print("CSV created successfully")
print(df.head(10))