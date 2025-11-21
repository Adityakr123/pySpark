import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# ==========================================================
# 1. Create directories
# ==========================================================
os.makedirs("/app/testdata", exist_ok=True)
os.makedirs("/app/output", exist_ok=True)

# ==========================================================
# 2. Create a correct 14-byte fixed-length COBOL file
# ==========================================================
records = [
    "0001JOHN DOE  ",   # 14 chars
    "0002ALICE     ",   # 14 chars
    "0003ROBERT    ",   # 14 chars
]

with open("/app/testdata/data.dat", "wb") as f:
    for r in records:
        f.write(r.encode("ascii"))

print("Created fixed-length COBOL data file:")
print(f"File size: {os.path.getsize('/app/testdata/data.dat')} bytes\n")
# Should print 42 bytes (3 × 14)

# ==========================================================
# 3. Create COBOL copybook
# ==========================================================
copybook = """
       01 RECORD.
          05 ID    PIC 9(4).
          05 NAME  PIC X(10).
"""

with open("/app/testdata/copybook.cob", "w") as f:
    f.write(copybook)

print("Copybook written.\n")

# ==========================================================
# 4. Start Spark with Delta Lake
# ==========================================================
builder = (
    SparkSession.builder.appName("DeltaCobolTest")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# ==========================================================
# 5. Read COBOL file via Spark-Cobol (Cobrix)
# ==========================================================
df_cobol = (
    spark.read.format("cobol")
    .option("copybook", "/app/testdata/copybook.cob")
    .option("schema_retention_policy", "collapse_root")
    .load("/app/testdata/data.dat")
)

print("===== COBOL RAW DATA =====")
df_cobol.show(truncate=False)

# ==========================================================
# 6. Write to Delta Lake
# ==========================================================
delta_path = "/app/output/delta_table"

df_cobol.write.format("delta").mode("overwrite").save(delta_path)

print("\nDelta Lake write completed.\n")

# ==========================================================
# 7. Read back Delta Lake table
# ==========================================================
print("===== DELTA TABLE OUTPUT =====")
df_delta = spark.read.format("delta").load(delta_path)
df_delta.show(truncate=False)

spark.stop()
