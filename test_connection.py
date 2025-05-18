from databricks.connect import DatabricksSession

# Initialize Databricks session with cluster ID
spark = (DatabricksSession.builder
         .remote(cluster_id="0518-042519-hlr6jo81")  # Replace with your actual cluster ID
         .getOrCreate())

# Test the connection by creating a simple DataFrame
test_df = spark.range(10)
print("Successfully created DataFrame:")
test_df.show()

# Print Spark version
print(f"Spark version: {spark.version}") 