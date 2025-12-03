from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, col

spark = SparkSession.builder.appName("ManifestListener").getOrCreate()

path = "/streamFiles"

# Read streaming files
df = spark.readStream.format("text").load(path)

# Add filename column
df = df.withColumn("fileName", input_file_name())

# Filter to ONLY look for files that contain the word 'manifest'
df = df.filter(col("fileName").contains("manifest"))

def process_batch(batch_df, batch_id):

    if batch_df.count() == 0:
        print("Waiting for manifest file...")
        return

    print("\n📄 Manifest file detected!\n")

    # Combine file content
    lines = [row["value"] for row in batch_df.collect()]
    content = "\n".join(lines)

    # Conditions
    gl_ok = "gl_balance successful" in content
    cbs_ok = "cbs_balance successful" in content

    # Check if both exist
    if gl_ok and cbs_ok:
        print("🔥 manifest file contains BOTH entries!")
        print("🔥 FINAL ACTION TRIGGERED!\n")
    else:
        print("⚠ manifest file missing required entries:")
        print("gl_balance successful :", gl_ok)
        print("cbs_balance successful:", cbs_ok)
        print()

query = df.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "/tmp/chk_manifest_only") \
    .start()

query.awaitTermination()
