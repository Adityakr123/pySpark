# # FROM nginx:latest

# #deltalake
# FROM sampath7997/spark-requests:v1

# USER root

# WORKDIR /opt/spark/jars

# # ------------------------------------------------------
# # Delta Lake 3.2.0 (Spark 3.5.x)
# # ------------------------------------------------------
# RUN wget https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar && \
#     wget https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar
# RUN pip install --no-cache-dir delta-spark

# # ------------------------------------------------------
# # Spark-Cobol (Cobrix) 2.8.0
# # ONLY 2 jars needed
# # ------------------------------------------------------
# RUN wget https://repo1.maven.org/maven2/za/co/absa/cobrix/spark-cobol_2.12/2.8.0/spark-cobol_2.12-2.8.0.jar && \
#     wget https://repo1.maven.org/maven2/za/co/absa/cobrix/cobol-parser_2.12/2.8.0/cobol-parser_2.12-2.8.0.jar

# # ------------------------------------------------------
# # Enable Delta Lake
# # ------------------------------------------------------
# RUN echo "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" >> /opt/spark/conf/spark-defaults.conf && \
#     echo "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" >> /opt/spark/conf/spark-defaults.conf

# USER mambauser
# WORKDIR /app

#delta lake latest

FROM sampath7997/spark-requests:v1

WORKDIR /app

# Install Delta Lake Python package
RUN pip install --no-cache-dir delta-spark==3.2.0

# Download Cobrix JARs into Spark jars folder
WORKDIR /opt/spark/jars

RUN wget https://repo1.maven.org/maven2/za/co/absa/cobrix/spark-cobol_2.12/2.8.0/spark-cobol_2.12-2.8.0.jar && \
    wget https://repo1.maven.org/maven2/za/co/absa/cobrix/cobol-parser_2.12/2.8.0/cobol-parser_2.12-2.8.0.jar && \
    wget https://repo1.maven.org/maven2/za/co/absa/cobrix/cobol-parser-common_2.12/2.8.0/cobol-parser-common_2.12-2.8.0.jar

USER mambauser
# Move back to working directory
WORKDIR /app