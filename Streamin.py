from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, col

spark = SparkSession.builder \
    .appName("ManifestListener") \
    .getOrCreate()

manifest_path = "hdfs:///path/to/manifest/"

# Read streaming files
df = spark.readStream \
    .format("text") \
    .load(manifest_path) \
    .withColumn("filename", input_file_name())

# Filter for specific file name pattern
filtered = df.filter(col("filename").contains("manifest"))

# Process the batch
def process_batch(batch_df, batch_id):
    if batch_df.count() == 0:
        return

    # Look for specific content inside the file
    matched = batch_df.filter(col("value").contains("YOUR_KEYWORD"))

    if matched.count() > 0:
        # 🧠 Your internal logic goes here:
        print(f"Batch {batch_id}: Required info found!")
        
        # Example internal logic (DB write, HDFS write, API call, etc.)
        # Here we just print the matched lines:
        rows = matched.collect()
        for r in rows:
            print(f"Matched line: {r['value']}")

        # You can add your custom logic here:
        # e.g. write to a table, update status, trigger pipeline steps, etc.

query = filtered.writeStream \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()
