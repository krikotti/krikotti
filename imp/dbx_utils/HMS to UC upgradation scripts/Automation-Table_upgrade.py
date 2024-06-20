# Databricks notebook source
# Note: provider adjusted for managed types

# COMMAND ----------

dbutils.widgets.text('hms_schema_name.table_name','hms_schema_name.table_name',' Input Table Path( Schema_name.table_name)')
dbutils.widgets.text('UC_Catalog_name','UC_Catalog_name')
dbutils.widgets.text('user_email','user_email')


# dbutils.widgets.remove('InputTablepath')
# dbutils.widgets.remove('user_email')
# dbutils.widgets.remove('UC_Catalog_name')

# COMMAND ----------

InputTableName = dbutils.widgets.get('hms_schema_name.table_name').strip()
print(InputTableName)
Catalog =  dbutils.widgets.get('UC_Catalog_name').strip()
print(Catalog)
user_email =   dbutils.widgets.get('user_email').strip()
print(user_email)

# COMMAND ----------

InputTable = spark.sql(f"SELECT * from {InputTableName}")
tables = [row['TABLENAME'] for row in InputTable.collect()]
schemas = [row['SCHEMANAME'] for row in InputTable.collect()]
log_catalog = Catalog

# COMMAND ----------

import re
import pyspark
from pyspark.sql import SparkSession
import itertools

# Create a Spark session
spark_session = SparkSession.builder.appName('Spark_Session').getOrCreate()
columns = ['schema_name', 'table_name', 'Status', 'Message']

# Initialize an empty list to store data

data = []
if(len(tables)!=len(schemas)):
    print("issue")
    raise ValueError("there is some issue while fetching the table and schema names. the input table should not have any table name without schema name ")

for i in range(len(tables)):
    spark.sql('Use catalog hive_metastore')
    try:
        Status = 'NOT UPGRADED'
        Type, Provider, Location, errr = "NA", "NA", "NA","NA"
        # Describe extended for table

        schema_name = schemas[i]
        table_name = tables[i]
        query = f"DESC EXTENDED `{schemas[i]}`.`{tables[i]}`"
        # print(query)
        table_details_df = spark.sql(query)
        details = table_details_df.collect()
        
        #put this in a functions it'll look good
        for detail in details:
            col_name = detail['col_name']
            data_type = detail['data_type']
            if ((col_name == 'Type')and(data_type == 'EXTERNAL' or data_type == 'MANAGED' or data_type == 'VIEW')):
                Type = data_type
            elif col_name == 'Provider' and data_type.lower() in ('csv', 'delta', 'text', 'parquet', 'json', 's3select','avro','orc'):
                Provider = data_type

            elif col_name == 'Location' and data_type.lower().startswith('s3'):
                Location = data_type
                
            elif Type.lower() == 'view':
                Location = 'view'
                Provider = 'view'
            
        
        if any(val == 'NA' for val in (Type, Provider, Location)):
            if(Location == 'NA'):
                raise ValueError('Either Location not found or the location is not an S3 location')
            elif(Type == 'NA'):
                raise ValueError('Type not found')
            elif(Provider == 'NA'):
                raise ValueError('Either provider not found or it does notbelong to one of the supported format(csv, delta, text, parquet, json,s3select,avro,orc)')
            # print('something not found')
            #be specific here
        else:
            upgrade_query = []
            if(Type == 'EXTERNAL'):
                upgrade_query = [f"CREATE TABLE {Catalog}.{schemas[i]}.{tables[i]} LIKE hive_metastore.{schemas[i]}.{tables[i]} COPY LOCATION;" ,f"ALTER TABLE hive_metastore.{schemas[i]}.{tables[i]}  SET TBLPROPERTIES ('upgraded_to' = '{Catalog}.{schemas[i]}.{tables[i]}');"]
                # print(upgrade_query)
                for cmd in (upgrade_query):
                    # print("cmd :",cmd)
                    df  = spark.sql(cmd)
                    # print(df)
            elif(Type == 'MANAGED'):
                upgrade_query = [f"CREATE TABLE {Catalog}.{schemas[i]}.{tables[i]} using Provider LOCATION '{Location}'", f"ALTER TABLE hive_metastore.{schemas[i]}.{tables[i]}  SET TBLPROPERTIES ('upgraded_to' = '{Catalog}.{schemas[i]}.{tables[i]}');"]

                # print(upgrade_query)
                for cmd in (upgrade_query):
                    # print("cmd :",cmd)
                    df  = spark.sql(cmd)
                    # print(df)
            elif(Type == 'VIEW'):
                df = spark.sql(f'show create table {schemas[i]}.{tables[i]}')
                val = df.collect()
                # print(val)
                create_table = val[0]['createtab_stmt']
                pattern = r'TBLPROPERTIES\s*\([^)]*\)'
                op = re.sub(pattern, '', create_table)
                spark.sql(f'USE CATALOG {Catalog}')
                df  = spark.sql(op)
                # print(df)
                spark.sql('Use catalog hive_metastore')
            Status = 'Upgraded'

            
    except Exception as err:
        errr =str(err)
        errr =errr[:300]

    finally:
        # print(Type, errr)
        data.append((schema_name, table_name, Status ,errr))
        print(schema_name, table_name, Status ,errr)
        spark.sql('Use catalog hive_metastore')

value_df = spark_session.createDataFrame(data, columns)


# COMMAND ----------

import datetime

current_timestamp = datetime.datetime.now()
current_timestamp = str(current_timestamp)
time = current_timestamp.replace(' ','_').replace(".","_").replace("-","_").replace(":","_")

path = f'{log_catalog}.Upgrade_logs.table_upgrade_log_{user_email.split(".")[0]}_{time}'
print(path)

# COMMAND ----------

batch_size = 200
data_list = value_df.collect()

# Function to push data to a table
def push_data_to_table(data_batch):
    # creating batch df's
    batch_df = spark.createDataFrame(data_batch, columns)

    # Append the data to table
    batch_df.write.mode("append").saveAsTable(path)

    # For this example, we'll just print the rows in the batch
    for row in data_batch:
        print("Pushing row to table:", row)

# Partition the list and process in batches
for i in range(0, len(data_list), batch_size):
    batch = data_list[i:i + batch_size]
    push_data_to_table(batch)

# Stop the SparkSession when you're done
# spark.stop()


# COMMAND ----------

df = spark.sql(f"GRANT SELECT ON {path} TO `{user_email}`")
print('1')
df = spark.sql(f"Grant Use schema on schema {log_catalog}.upgrade_logs to `{user_email}`")
print('2')
df = spark.sql(f"grant use catalog on catalog {log_catalog} to `{user_email}`")
