from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col,when

def main():
    spark = SparkSession.builder .appName("LocalFile_to_Oracle_ETL").config("spark.jars", "/usr/local/spark/jars/ojdbc8.jar").getOrCreate()

    # Read file (assuming space-separated)
    input_path = "data.txt"
    df_raw = spark.read.text(input_path)

    # Extract Id, Amount, Type
    df_clean = df_raw.withColumn("Id", expr("substring(value, 1, 18)")) \
                     .withColumn("Amount", expr("substring(value, 24, length(value)-24)")) \
                     .withColumn("Type", expr("substring(value, length(value), 1)")) \
                     .drop("value")

    print("=== Transformed Data ===")
    df_clean.show(truncate=False)
    print(df_clean)


    df_clean = df_clean.withColumn(
        "Type",
        when(col("Type") == "+", "positive")
        .when(col("Type") == "-", "negative")
        .otherwise(col("Type"))
    )

    print("=== final df ===")
    df_clean.show(truncate=False)
    # Write to Oracle DB
    oracle_url = "jdbc:oracle:thin:@//localhost:1521/XEPDB1"
    oracle_user = "system"
    oracle_password = "oracle"

    df_clean.write \
        .format("jdbc") \
        .option("url", oracle_url) \
        .option("dbtable", "indb") \
        .option("user", oracle_user) \
        .option("password", oracle_password) \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .mode("append") \
        .save()

    print("=== Data successfully written to Oracle DB ===")
    spark.stop()

if __name__ == "__main__":
    main()
