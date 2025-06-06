from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType

database_name = "<catalog.schema>"
tables = spark.sql(f"SHOW TABLES IN {database_name}").collect()

results = []

for table in tables:
    table_name = table.tableName
    try:
        # Get table type information
        table_type_query = f"""
        SELECT table_type
        FROM system.information_schema.tables
        WHERE table_schema = '{database_name.split('.')[1]}'
          AND table_catalog = '{database_name.split('.')[0]}'
          AND table_name = '{table_name}'
        """
        table_type_info = spark.sql(table_type_query).collect()
        table_type = table_type_info[0].table_type if table_type_info else "Unknown"
        
        table_info = spark.sql(f"DESCRIBE DETAIL {database_name}.{table_name}").collect()
        if table_info and table_info[0].format == "delta":
            table_size = table_info[0].sizeInBytes / (1024**4)
            num_files = table_info[0].numFiles   # Convert to thousands
            name = table_info[0].name
            lastModified = table_info[0].lastModified
            partition_columns = table_info[0].partitionColumns if table_info[0].partitionColumns else []
            
            # Get column information
            columns_info = spark.sql(f"DESCRIBE TABLE {database_name}.{table_name}").collect()
            columns = [col.col_name for col in columns_info if col.col_name not in ('# Partition Information', '')]
            
            history_df = spark.sql(f"DESCRIBE HISTORY {database_name}.{table_name}")
            operations = history_df.select("operation", "timestamp").collect()
            
            optimize_applied = "Yes" if any(row.operation == "OPTIMIZE" for row in operations) else "No"
            zorder_applied = "Yes" if any("zOrderBy" in str(entry.operationParameters) for entry in history_df.filter("operation = 'OPTIMIZE'").select("operationParameters").collect()) else "No"
            vacuum_applied = "Yes" if any(row.operation == "VACUUM END" for row in operations) else "No"
            
            optimize_timestamp = next((row.timestamp for row in operations if row.operation == "OPTIMIZE"), "N/A")
            vacuum_timestamp = next((row.timestamp for row in operations if row.operation == "VACUUM END"), "N/A")
            
            # Determine last used timestamp
            last_used_timestamp = max(row.timestamp for row in operations) if operations else "N/A"
            
            # Determine last queried timestamp
            last_queried_timestamp = next((row.timestamp for row in operations if row.operation == "READ"), "N/A")
            
            zorder_columns = []
            if optimize_applied == "Yes":
                latest_zorder_entry = history_df.filter("operation = 'OPTIMIZE'") \
                                                .orderBy("timestamp", ascending=False) \
                                                .select("operationParameters") \
                                                .first()
                if latest_zorder_entry and "zOrderBy" in str(latest_zorder_entry.operationParameters):
                    operation_parameters = latest_zorder_entry.operationParameters
                    zorder_columns = operation_parameters.get("zOrderBy", [])
                    if not isinstance(zorder_columns, list):
                        zorder_columns = [zorder_columns]
            
            results.append(Row(
                table_name=name,
                table_size_TB=table_size,
                num_files=num_files,
                lastModified=lastModified,
                optimize_applied=optimize_applied,
                optimize_timestamp=optimize_timestamp,
                zorder_applied=zorder_applied,
                vacuum_applied=vacuum_applied,
                vacuum_timestamp=vacuum_timestamp,
                zorder_columns=zorder_columns,
                partition_columns=partition_columns,
                table_type=table_type,
                last_used_timestamp=last_used_timestamp,
                last_queried_timestamp=last_queried_timestamp,
                columns=columns
            ))
        else:
            results.append(Row(
                table_name=name,
                table_size_TB=None,
                num_files=None,
                lastModified="N/A",
                optimize_applied="N/A",
                optimize_timestamp="N/A",
                zorder_applied="N/A",
                vacuum_applied="N/A",
                vacuum_timestamp="N/A",
                zorder_columns=[],
                partition_columns=[],
                table_type=table_type,
                last_used_timestamp="N/A",
                last_queried_timestamp="N/A",
                columns=[]
            ))
    except Exception as e:
        results.append(Row(
            table_name=table_name,
            table_size_TB=None,
            num_files=None,
            lastModified="Error",
            optimize_applied="Error",
            optimize_timestamp="Error",
            zorder_applied="Error",
            vacuum_applied="Error",
            vacuum_timestamp="Error",
            zorder_columns=[],
            partition_columns=[],
            table_type="Error",
            last_used_timestamp="Error",
            last_queried_timestamp="Error",
            columns=[]
        ))

schema = StructType([
    StructField("table_name", StringType(), True),
    StructField("table_size_TB", DoubleType(), True),
    StructField("num_files", DoubleType(), True),
    StructField("lastModified", StringType(), True),
    StructField("optimize_applied", StringType(), True),
    StructField("optimize_timestamp", StringType(), True),
    StructField("zorder_applied", StringType(), True),
    StructField("vacuum_applied", StringType(), True),
    StructField("vacuum_timestamp", StringType(), True),
    StructField("zorder_columns", ArrayType(StringType()), True),
    StructField("partition_columns", ArrayType(StringType()), True),
    StructField("table_type", StringType(), True),
    StructField("last_used_timestamp", StringType(), True),
    StructField("last_queried_timestamp", StringType(), True),
    StructField("columns", ArrayType(StringType()), True)
])

results_df = spark.createDataFrame(results, schema)
display(results_df)