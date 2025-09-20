"""
Structured Streaming example converted from the notebook
Smart Building HVAC Monitoring - Structured Streaming (console output)

This script includes Windows diagnostics for HADOOP_HOME/winutils and wraps
awaitTermination calls to print Python + Java exception info for debugging.

Run this with the same Python environment you use for the notebook (PySpark installed).
"""

import os
import sys
import time
import traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, rand, when


def setup_windows_hadoop_defaults():
    """Set HADOOP_HOME defaults and disable file locking on Windows; report winutils status."""
    # Adjust this path if you have a local Spark/Hadoop installation or winutils location
    default_hadoop_home = r"C:\Users\farra\spark\spark-4.0.0-bin-hadoop3"
    os.environ.setdefault("HADOOP_HOME", default_hadoop_home)
    os.environ.setdefault("HADOOP_CLIENT_DISABLE_FILE_LOCKING", "true")

    winutils_path = os.path.join(os.environ.get("HADOOP_HOME", ""), "bin", "winutils.exe")
    if os.path.exists(winutils_path):
        os.environ["PATH"] = os.path.dirname(winutils_path) + os.pathsep + os.environ.get("PATH", "")
        print(f"Found winutils.exe at: {winutils_path}")
    else:
        print("winutils.exe not found at:", winutils_path)
        print("If you're on Windows, download a winutils.exe that matches your Hadoop version and place it at HADOOP_HOME\\bin.")
        print("Common fixes: set HADOOP_HOME to the folder containing bin\\winutils.exe, or run Spark inside WSL/Docker to avoid Windows native IO.")

    print("HADOOP_HOME=", os.environ.get("HADOOP_HOME"))
    print("HADOOP_CLIENT_DISABLE_FILE_LOCKING=", os.environ.get("HADOOP_CLIENT_DISABLE_FILE_LOCKING"))


def create_spark_session(app_name="Smart Building HVAC Monitoring"):
    # Enable Python faulthandler in worker and UDF execution to get better tracebacks
    # See: spark.sql.execution.pyspark.udf.faulthandler.enabled and spark.python.worker.faulthandler.enabled
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
        .config("spark.python.worker.faulthandler.enabled", "true")
    )
    return builder.getOrCreate()


def simulate_sensor_stream(spark, rows_per_second=5):
    # Use Spark's rate source to simulate data
    sensor_data = (
        spark.readStream.format("rate").option("rowsPerSecond", rows_per_second).load()
        .withColumn("room_id", expr("CAST(value % 10 AS STRING)"))
        .withColumn("temperature",
                    when(expr("value % 10 == 0"), 15)
                    .otherwise(20 + rand() * 25))
        .withColumn("humidity", expr("40 + rand() * 30"))
    )
    return sensor_data


# SQL queries from the notebook
CRITICAL_TEMPERATURE_QUERY = """
    SELECT 
        room_id, 
        temperature, 
        humidity, 
        timestamp 
    FROM sensor_table 
    WHERE temperature < 18 OR temperature > 60
"""

AVERAGE_READINGS_QUERY = """
    SELECT 
        room_id, 
        AVG(temperature) AS avg_temperature, 
        AVG(humidity) AS avg_humidity, 
        window.start AS window_start 
    FROM sensor_table
    GROUP BY room_id, window(timestamp, '1 minute')
"""

ATTENTION_NEEDED_QUERY = """
    SELECT 
        room_id, 
        COUNT(*) AS critical_readings 
    FROM sensor_table 
    WHERE humidity < 45 OR humidity > 75
    GROUP BY room_id
"""


def main():
    # Windows diagnostics
    if sys.platform.startswith("win"):
        setup_windows_hadoop_defaults()

    spark = create_spark_session()

    # Print Spark/Hadoop versions for debugging
    try:
        print("Spark version:", spark.version)
        hadoop_version = spark.sparkContext._jvm.org.apache.hadoop.util.VersionInfo.getVersion()
        print("Hadoop version:", hadoop_version)
    except Exception:
        # If JVM isn't available for some reason, keep going
        traceback.print_exc()

    sensor_data = simulate_sensor_stream(spark, rows_per_second=5)
    sensor_data.createOrReplaceTempView("sensor_table")

    # Create streaming DataFrames using SQL
    critical_temperatures_stream = spark.sql(CRITICAL_TEMPERATURE_QUERY)
    average_readings_stream = spark.sql(AVERAGE_READINGS_QUERY)
    attention_needed_stream = spark.sql(ATTENTION_NEEDED_QUERY)

    # Start output streams to console
    critical_query = (
        critical_temperatures_stream.writeStream
        .outputMode("append")
        .format("console")
        .queryName("Critical Temperatures")
        .start()
    )

    average_query = (
        average_readings_stream.writeStream
        .outputMode("complete")
        .format("console")
        .queryName("Average Readings")
        .start()
    )

    attention_query = (
        attention_needed_stream.writeStream
        .outputMode("complete")
        .format("console")
        .queryName("Attention Needed")
        .start()
    )

    # Keep streams running and surface errors clearly
    try:
        print("********Critical Temperature Values*******")
        critical_query.awaitTermination()
    except Exception as e:
        print("StreamingQueryException or other error while running critical_query:")
        traceback.print_exc()
        try:
            print("Java exception object:", e.java_exception)
        except Exception:
            pass

    try:
        print("********Average Readings Values********")
        average_query.awaitTermination()
    except Exception as e:
        print("StreamingQueryException or other error while running average_query:")
        traceback.print_exc()
        try:
            print("Java exception object:", e.java_exception)
        except Exception:
            pass

    try:
        print("********Attention Needed Values********")
        attention_query.awaitTermination()
    except Exception as e:
        print("StreamingQueryException or other error while running attention_query:")
        traceback.print_exc()
        try:
            print("Java exception object:", e.java_exception)
        except Exception:
            pass

    # Stop Spark
    spark.stop()


if __name__ == "__main__":
    main()
