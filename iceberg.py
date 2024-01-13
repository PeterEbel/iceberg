from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import DoubleType, FloatType, LongType, StructType,StructField, StringType
 
catalog = "iceberg"
namespace ="common"
table = "customers"
change_log_view = "view_customer_changes"
csv_base_path = "/home/peter/projects/iceberg/data/"

partitions = ["2024-01-01", "2024-02-01", "2024-03-01", "2024-04-01", "2024-05-01", "2024-06-01"]
 
def cleanup():
    spark.sql(f"""DROP TABLE IF EXISTS {catalog}.{namespace}.{table};""")
    spark.sql(f"""DROP NAMESPACE IF EXISTS {catalog}.{namespace};""")

# Initialize Spark session with Iceberg support
spark = SparkSession \
    .builder \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config(f"""spark.sql.catalog.{catalog}""", "org.apache.iceberg.spark.SparkCatalog") \
    .config(f"""spark.sql.catalog.{catalog}.type""", "hive") \
    .config(f"""spark.sql.catalog.{catalog}.uri""" , "thrift://0.0.0.0:9083") \
    .config("spark.sql.debug.maxToStringFields" , "255") \
    .getOrCreate()

# Create database and Iceberg table
# cleanup()
spark.sql(f"""CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace};""")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{namespace}.{table}
      (
        entity_id STRING,
        customer_number STRING,
        valid_from_date STRING,
        valid_to_date STRING, 
        gender_code STRING,
        last_name STRING,
        first_name STRING,
        birth_date STRING,
        country_code STRING,
        postal_code STRING,
        city STRING,
        street STRING
      ) 
      USING Iceberg;""")
 
customer_schema = StructType([
    StructField("entity_id", StringType(), True),
    StructField("customer_number", StringType(), True),
    StructField("valid_from_date", StringType(), True),
    StructField("valid_to_date", StringType(), True),
    StructField("gender_code", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("birth_date", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("city", StringType(), True),
    StructField("street", StringType(), True)
])

# Append CSV partitions to the Iceberg table
# for p in partitions:
#     df_customers = spark.read.options(delimiter="|", header=True).schema(customer_schema).csv(f"""{csv_base_path}/{p}.csv""")
#     df_customers.writeTo(f"""{catalog}.{namespace}.{table}""").append()

# # Read and execute INSERT statements
# df_customer_inserts = spark.read.text(f"""{csv_base_path}/customer_inserts.csv""") 
# ls_customer_inserts = df_customer_inserts.collect()
# for i in ls_customer_inserts:
#     spark.sql(i[0])

# # Read and execute UPDATE statements
# df_customer_updates = spark.read.text(f"""{csv_base_path}/customer_updates.csv""") 
# ls_customer_updates = df_customer_updates.collect()
# for u in ls_customer_updates:
#     spark.sql(u[0])

# Merge records from a source table into existing target table
for p in partitions:
    df_customers_source = spark.read.options(delimiter="|", header=True).schema(customer_schema).csv(f"""{csv_base_path}/{p}.csv""")
    df_customers_source.createOrReplaceTempView("customers_source_view")
    spark.sql(f"""MERGE INTO {catalog}.{namespace}.{table} AS ct
                  USING customers_source_view AS cs
                    ON ct.customer_number = cs.customer_number
                  WHEN MATCHED AND cs.customer_number IS NULL THEN
                    DELETE
                  WHEN MATCHED THEN
                    UPDATE
                      SET 
                        ct.entity_id = cs.entity_id,
                        ct.customer_number = cs.customer_number,
                        ct.valid_from_date = cs.valid_from_date,
                        ct.valid_to_date = cs.valid_to_date,
                        ct.gender_code = cs.gender_code,
                        ct.last_name = cs.last_name,
                        ct.first_name = cs.first_name,
                        ct.birth_date = cs.birth_date,
                        ct.country_code = cs.country_code,
                        ct.postal_code = cs.postal_code,
                        ct.city = cs.city,
                        ct.street = cs.street
                  WHEN NOT MATCHED THEN
                    INSERT
                      ( 
                        ct.entity_id,
                        ct.customer_number,
                        ct.valid_from_date,
                        ct.valid_to_date,
                        ct.gender_code,
                        ct.last_name,
                        ct.first_name,
                        ct.birth_date,
                        ct.country_code,
                        ct.postal_code,
                        ct.city,
                        ct.street
                      )
                    VALUES
                      (
                        cs.entity_id,
                        cs.customer_number,
                        cs.valid_from_date,
                        cs.valid_to_date,
                        cs.gender_code,
                        cs.last_name,
                        cs.first_name,
                        cs.birth_date,
                        cs.country_code,
                        cs.postal_code,
                        cs.city,
                        cs.street
                      )""")
 
# Snapshot-History and Tagging
print()
print("Snapshot-History")
print("================")
print(f"""SELECT snapshot_id FROM {catalog}.{namespace}.{table}.history ORDER BY made_current_at ASC;""")
snapshots = spark.sql(f"""SELECT snapshot_id FROM {catalog}.{namespace}.{table}.history ORDER BY made_current_at ASC;""")
i = 0
for s in snapshots.collect():
    print(str("{:03d}".format(i)).rjust(3) +"  " + str(s[0]).rjust(19))
    # Hyphens are not allowed in TAG names!
    spark.sql(f"""ALTER TABLE {catalog}.{namespace}.{table} CREATE TAG {partitions[i].replace('-', '_')} AS OF VERSION {s[0]}""")
    i = i + 1

# First and last snapshot
print()
print("First Snapshot: " + str(snapshots.head(1)[0][0]).rjust(19))
print("Last  Snapshot: " + str(snapshots.tail(1)[0][0]).rjust(19))

# Snapshot-Details
print()
print("Snapshot-Details")
print("================")
print(f"""SELECT * FROM {catalog}.{namespace}.{table}.snapshots;""")
spark.sql(f"""SELECT * FROM {catalog}.{namespace}.{table}.snapshots;""").show(truncate=False)

# Snapshot-Changelog via procedure
if (snapshots.head(1)[0][0] != snapshots.tail(1)[0][0]):
    df_diff = spark.sql(f"""
    CALL {catalog}.system.create_changelog_view(
        table => '{namespace}.{table}',
        options => map('start-snapshot-id', '{snapshots.head(1)[0][0]}', 'end-snapshot-id', '{snapshots.tail(1)[0][0]}'),
        changelog_view => '{change_log_view}'
    )
    """)
    print()
    print("Snapshot-Changelog")
    print("==================")
    print(f"""SELECT * FROM {change_log_view} ORDER BY last_name ASC, _change_ordinal;""")
    spark.sql(f"""SELECT * FROM {change_log_view} ORDER BY last_name ASC, _change_ordinal;""").show()

print()
print("Table-Changes")
print("=============")
print(f"""SELECT * FROM {catalog}.{namespace}.{table}.changes ORDER BY last_name ASC, _change_ordinal;""")
spark.sql(f"""SELECT * FROM {catalog}.{namespace}.{table}.changes ORDER BY last_name ASC, _change_ordinal;""").show()

print()
print("Parquet files composing the table")
print("=================================")
print(f"""SELECT * FROM {catalog}.{namespace}.{table}.files;""")
spark.sql(f"""SELECT * FROM {catalog}.{namespace}.{table}.files;""").show(truncate=False)

print()
print("Metadata Log Entries")
print("====================")
print(f"""SELECT * FROM {catalog}.{namespace}.{table}.metadata_log_entries;""")
df = spark.sql(f"""SELECT * FROM {catalog}.{namespace}.{table}.metadata_log_entries;""")
df.show(truncate=False)

print()
print("Query Table")
print("===========")
print(f"""SELECT * FROM {catalog}.{namespace}.{table} ORDER BY last_name;""")
df = spark.sql(f"""SELECT * FROM {catalog}.{namespace}.{table} ORDER BY last_name;""")
df.show(truncate=False)

print()
print("Query tagged snapshots")
print("======================")
i = 0
for p in partitions:
  print(f"""SELECT * FROM {catalog}.{namespace}.{table} FOR VERSION AS OF {p.replace('-', '_')} ORDER BY last_name;""")
  df = spark.sql(f"""SELECT * FROM {catalog}.{namespace}.{table} FOR VERSION AS OF '{p.replace('-', '_')}' ORDER BY last_name;""").show(truncate=False)
  i = i + 1
  print()

# df = spark.read.format("iceberg").load(f"""{catalog}.{namespace}.{table}""")
