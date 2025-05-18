from databricks.connect import DatabricksSession
import requests
import re
import yaml
from io import StringIO
import pandas as pd

# Load configuration
with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)

# Initialize Databricks session
spark = (DatabricksSession.builder
         .remote(cluster_id=config['databricks']['cluster_id'])
         .getOrCreate())

# Load data from URL
csv = requests.get("https://raw.githubusercontent.com/IBM/telco-customer-churn-on-icp4d/master/data/Telco-Customer-Churn.csv").text
df = pd.read_csv(StringIO(csv), sep=",")

def cleanup_column(pdf):
    # Clean up column names
    pdf.columns = [re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower().replace("__", "_") for name in pdf.columns]
    pdf.columns = [re.sub(r'[\(\)]', '', name).lower() for name in pdf.columns]
    pdf.columns = [re.sub(r'[ -]', '_', name).lower() for name in pdf.columns]
    return pdf.rename(columns = {'streaming_t_v': 'streaming_tv', 'customer_i_d': 'customer_id'})

# Apply cleanup to the dataframe
df = cleanup_column(df)

# Convert pandas DataFrame to Spark DataFrame and write to table
table_path = f"{config['databricks']['catalog']}.{config['databricks']['schema']}.{config['databricks']['table']}"
spark.createDataFrame(df).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_path)
print(f"Data written to table: {table_path}")