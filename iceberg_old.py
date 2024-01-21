from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import DoubleType, FloatType, LongType, StructType,StructField, StringType
 
catalog = "iceberg"
namespace ="common"
table = "customers"
change_log_view = "view_customer_changes"
csv_base_path = "/home/peter/projects/iceberg/data/"

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

# partitions = ["2024-01-01", "2024-02-01", "2024-03-01", "2024-04-01", "2024-05-01", "2024-06-01"]
partitions = ["2024-01-01", "2024-03-01"]

def create_database():
    spark.sql(f"""CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace};""")

def create_table():
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
        USING Iceberg
        TBLPROPERTIES (
            'format-version' = '2', -- allow merge-on-read if needed
            'write.metadata.delete-after-commit.enabled'='true'
        );
        """)

def cleanup():
    spark.sql(f"""DROP TABLE IF EXISTS {catalog}.{namespace}.{table};""")
    spark.sql(f"""DROP NAMESPACE IF EXISTS {catalog}.{namespace};""")

def merge_partition():
    spark.sql(f"""
        WITH changes AS (
          SELECT 
            cs.entity_id AS entity_id,
            COALESCE(cs.customer_number, ct.customer_number) AS customer_number,
            cs.valid_from_date AS valid_from_date,
            cs.valid_to_date AS valid_to_date,
            cs.gender_code AS gender_code,
            cs.last_name AS last_name,
            cs.first_name AS first_name,
            cs.birth_date AS birth_date,
            cs.country_code AS country_code,
            cs.postal_code AS postal_code,
            cs.city AS city,
            cs.street as street,
            CASE WHEN cs.customer_number IS NULL THEN 'D' WHEN ct.customer_number IS NULL THEN 'I' ELSE 'U' END as cdc
          FROM {catalog}.{namespace}.{table} ct
          FULL OUTER JOIN customers_source_view cs ON ct.customer_number = cs.customer_number
            WHERE (
              ct.entity_id <> cs.entity_id OR
              ct.valid_from_date <> cs.valid_from_date OR
              ct.valid_to_date <> cs.valid_to_date OR
              ct.gender_code <> cs.gender_code OR
              ct.last_name <> cs.last_name OR
              ct.first_name <> cs.first_name OR
              ct.birth_date <> cs.birth_date OR
              ct.country_code <> cs.country_code OR
              ct.postal_code <> cs.postal_code OR
              ct.city <> cs.city OR
              ct.street <> cs.street OR
              ct.customer_number IS NULL OR
              cs.customer_number IS NULL
          )
        )

      MERGE INTO {catalog}.{namespace}.{table} AS iceberg
      USING changes
        ON iceberg.customer_number = changes.customer_number
      WHEN MATCHED AND changes.cdc = 'U' THEN UPDATE SET *
      WHEN NOT MATCHED AND changes.cdc = 'I' THEN INSERT *
      WHEN NOT MATCHED BY SOURCE THEN DELETE
    """)
    
# Initialize Spark session with Iceberg support
spark = SparkSession \
    .builder \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config(f"""spark.sql.catalog.{catalog}""", "org.apache.iceberg.spark.SparkCatalog") \
    .config(f"""spark.sql.catalog.{catalog}.type""", "hive") \
    .config(f"""spark.sql.catalog.{catalog}.uri""" , "thrift://0.0.0.0:9083") \
    .config("spark.sql.debug.maxToStringFields" , "255") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Create database and Iceberg table
# cleanup()
create_database()
create_table()

# Merge records from a source table into existing target table
for p in partitions:
    df_customers_source = spark.read.options(delimiter="|", header=True).schema(customer_schema).csv(f"""{csv_base_path}/{p}.csv""")
    df_customers_source.createOrReplaceTempView("customers_source_view")
    merge_partition()

# Snapshot-History and Tagging
print()
print("Snapshot-History")
print("================")
print(f"""SELECT snapshot_id FROM {catalog}.{namespace}.{table}.history ORDER BY made_current_at ASC;""")
print()
snapshots = spark.sql(f"""SELECT snapshot_id FROM {catalog}.{namespace}.{table}.history ORDER BY made_current_at ASC;""")
i = 0
for s in snapshots.collect():
    # Hyphens are not allowed in TAG names!
    partition =  partitions[i].replace('-', '_')
    spark.sql(f"""ALTER TABLE {catalog}.{namespace}.{table} CREATE TAG {partition} AS OF VERSION {s[0]}""")
    print(str("{:03d}".format(i)).rjust(3) + "  " + str(s[0]).rjust(19) + "  " + partitions[i].replace('-', '_'))
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
spark.sql(f"""SELECT * FROM {catalog}.{namespace}.{table}.changes ORDER BY last_name ASC, _change_ordinal;""").show(n=100)

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
for p in partitions:
  partition = p.replace('-', '_')
  print(f"""SELECT * FROM {catalog}.{namespace}.{table} FOR VERSION AS OF {partition} ORDER BY last_name;""")
  df = spark.sql(f"""SELECT * FROM {catalog}.{namespace}.{table} FOR VERSION AS OF '{partition}' ORDER BY last_name;""").show(truncate=False)
  print()

# df = spark.read.format("iceberg").load(f"""{catalog}.{namespace}.{table}""")
