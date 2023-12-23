from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import DoubleType, FloatType, LongType, StructType,StructField, StringType
 
catalog = "iceberg"
namespace ="common"
table = "customers"
change_log_view = "view_customer_changes"
csv_base_path = "/home/peter/projects/iceberg/data/"
partitions = ["2024-01-01", "2024-02-01", "2024-03-01", "2024-04-01", "2024-05-01", "2024-06-01"]
 
def getSnapshots():
    df = spark.sql(f"""SELECT * FROM {catalog}.{namespace}.{table}.history ORDER BY made_current_at ASC;""")
    return df.collect()
 
# initialize Spark session with Iceberg support
spark = SparkSession \
    .builder \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config(f"""spark.sql.catalog.{catalog}""", "org.apache.iceberg.spark.SparkCatalog") \
    .config(f"""spark.sql.catalog.{catalog}.type""", "hive") \
    .config(f"""spark.sql.catalog.{catalog}.uri""" , "thrift://0.0.0.0:9083") \
    .getOrCreate()

spark.sql(f"""CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace};""")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{namespace}.{table} (
        entity_id STRING, \
        customer_number STRING, \
        valid_from_date STRING, \
        valid_to_date STRING, 
        gender_code STRING,
        last_name STRING,
        first_name STRING,
        birth_date STRING,
        country_code STRING,
        postal_code STRING,
        city STRING,
        street STRING,
        data_date_part STRING);
""")
 
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
    StructField("street", StringType(), True),
    StructField("data_date_part", StringType(), True)
])

# read the CSV partitions and append them to the Iceberg table
for p in partitions:
 
    df_customers = spark.read.options(delimiter="|", header=True).schema(customer_schema).csv(f"""{csv_base_path}/{p}.csv""")
    df_customers.writeTo(f"""{catalog}.{namespace}.{table}""").append()
 
snapshots = getSnapshots()
 
# i = 0
# for s in snapshots:
#     print(str(s[1]).rjust(19), s[0])
#     if i == 0:
#         previous_snapshot_id = s[1]
#         i = 1
#     else:
#         current_snapshot_id = s[1]
#         df_diff = spark.sql(f"""
#             CALL {catalog}.system.create_changelog_view(
#                 table => '{namespace}.{table}',
#                 options => map('start-snapshot-id', '{previous_snapshot_id}', 'end-snapshot-id', '{current_snapshot_id}'),
#                 changelog_view => '{change_log_view}'
#             )
#         """)
#         spark.sql(f"""SELECT * FROM {change_log_view};""").show()
#         previous_snapshot_id = current_snapshot_id
#         print(df_diff.collect())

for s in snapshots:
    print(str(s[1]).rjust(19), s[0])

df_diff = spark.sql(f"""
CALL {catalog}.system.create_changelog_view(
    table => '{namespace}.{table}',
    options => map('start-snapshot-id', '4047867018748105073', 'end-snapshot-id', '5685361918325468006'),
    changelog_view => '{change_log_view}'
)
""")

spark.sql(f"""SELECT * FROM {change_log_view};""").show()
