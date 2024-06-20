# Databricks notebook source
dbutils.widgets.text('InputTablepath','Schema_name.table_name',' Input Table Path( Schema_name.table_name)')
dbutils.widgets.text('UC_Catalog_name','UC Catalog name')
dbutils.widgets.text('user_email','user_email')
# dbutils.widgets.remove('user_email')

# COMMAND ----------

InputTableName = dbutils.widgets.get('InputTablepath').strip()
Catalog =  dbutils.widgets.get('UC_Catalog_name').strip()
user_email =   dbutils.widgets.get('user_email').strip()
if(user_email == ''):
    raise ValueError('user_email cannot be empty')

log_catalog = Catalog

# COMMAND ----------

InputTable = spark.sql(f"SELECT * from {InputTableName}")

if 'SCHEMANAME' not in InputTable.columns:
    raise ValueError("Error: 'SCHEMANAME' is not found in the columns of the Input table. Please refer the Table upgrade documentaion's Prerequisites section on Input Table creation.")
if 'TABLENAME' not in InputTable.columns:
    raise ValueError("Error: 'TABLENAME' is not found in the columns of the Input table. Please refer the Table upgrade documentaion's Prerequisites section on Input Table creation")

schemas = [row['SCHEMANAME'] for row in InputTable.collect()]
tables = [row['TABLENAME'] for row in InputTable.collect()]
print(tables)
print(schemas)
unique_values = list(set(schemas))
print(unique_values)


# COMMAND ----------

def sql_query_run(query):
    errr ='NA'
    try:
        df = spark.sql(query)            
        Status = 'Successful'
            
    except Exception as e:
        errr =str(e)
        errr =errr[:300]
        Status = 'Failed'
    finally:
        print(query,Status,errr)
        return query,Status,errr

# COMMAND ----------

# DBTITLE 1,Schema Permissions
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

data =[]

for schema in unique_values:
    
    df = spark.sql(f'show grants on schema {schema}')
    # Check for 'OWN' ActionType and create the 'owner' variable
    owner = df.filter(df['ActionType'] == 'OWN').select('Principal').collect()
    owner = owner[0][0] if owner else 'Admin'
    
    # Filter the DataFrame to retain rows with ObjectType 'DATABASE' and not 'OWN'
    filtered_df = df.filter((df['ObjectType'] == 'DATABASE') & (df['ActionType'] != 'OWN'))
    
    # Group by Principal and collect the ActionType values into an array
    result = filtered_df.groupBy('Principal').agg(F.collect_list('ActionType').alias('ActionTypes'))
    
    # Show the 'owner' variable and the result
    # print("Owner:", owner)
    # print(result.collect())
    for row in result.collect():
        principal = row['Principal']
        action_types = row['ActionTypes']
        
        # Skip ActionType 'READ_METADATA'
        action_types = [action for action in action_types if action != 'READ_METADATA']
        
        # Replace ActionType 'USAGE' with 'USE SCHEMA'
        action_types = ['USE SCHEMA' if action == 'USAGE' else action for action in action_types]

        #generating grant statements for catalog
        catalog_grant_statement = f"GRANT USE CATALOG ON CATALOG {Catalog} TO `{principal}`"
        # print(catalog_grant_statement)
        response =sql_query_run(catalog_grant_statement)
        data.append(response)
        # print (response, type(response),data)

        
        # Generate the grant statement for schema
        if action_types:
            grant_statement = f"GRANT {', '.join(action_types)} ON SCHEMA {Catalog}.{schema} TO `{principal}`;"
            print(grant_statement)
            response =sql_query_run(grant_statement)
            data.append(response)
        

    # Grant statement for the owner
    owner_grant_statement = f"ALTER SCHEMA {Catalog}.{schema} OWNER TO `{owner}`;"
    print(owner_grant_statement)
    response =sql_query_run(owner_grant_statement)
    data.append(response)

print(data)


# COMMAND ----------

# DBTITLE 1,Table permissions
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

for i in range(len(tables)):
    df = spark.sql(f"show grants on {schemas[i]}.{tables[i]}")
    # Check for 'OWN' ActionType and create the 'owner' variable
    owner = df.filter(df['ActionType'] == 'OWN').select('Principal').collect()
    owner = owner[0][0] if owner else 'Admin'
    
    # Filter the DataFrame to retain rows with ObjectType 'DATABASE' and not 'OWN'
    filtered_df = df.filter((df['ObjectType'] == 'TABLE') & (df['ActionType'] != 'OWN'))
    
    # Group by Principal and collect the ActionType values into an array
    result = filtered_df.groupBy('Principal').agg(F.collect_list('ActionType').alias('ActionTypes'))
    
    # Show the 'owner' variable and the result
    # print("Owner:", owner)
    # print(result.collect())
    for row in result.collect():
        principal = row['Principal']
        action_types = row['ActionTypes']
        
        # Skip ActionType 'READ_METADATA'
        action_types = [action for action in action_types if action != 'READ_METADATA']
        

        #generating grant statements for catalog and schema
        catalog_grant_statement = f"GRANT USE CATALOG ON CATALOG {Catalog} TO `{principal}`"
        schema_grant_statement = f"GRANT USE SCHEMA ON SCHEMA {Catalog}.{schemas[i]} TO `{principal}`"
        # print(catalog_grant_statement)
        response =sql_query_run(catalog_grant_statement)
        data.append(response)
        # print(schema_grant_statement)
        response =sql_query_run(schema_grant_statement)
        data.append(response)
        
        # Generate the grant statement for schema
        if action_types:
            grant_statement = f"GRANT {', '.join(action_types)} ON  {Catalog}.{schemas[i]}.{tables[i]} TO `{principal}`;"
            # print(grant_statement)
            response =sql_query_run(grant_statement)
            data.append(response)

    # Grant statement for the owner
    owner_grant_statement = f"ALTER TABLE {Catalog}.{schemas[i]}.{tables[i]} OWNER TO `{owner}`;"
    # print(owner_grant_statement)
    response =sql_query_run(owner_grant_statement)
    data.append(response)

print(data)

# COMMAND ----------

import re
import pyspark
from pyspark.sql import SparkSession
import itertools
import datetime

# Create a Spark session
spark_session = SparkSession.builder.appName('Spark_Session').getOrCreate()
columns = ['query', 'Status', 'Error_Message']

value_df = spark_session.createDataFrame(data, columns)

current_timestamp = datetime.datetime.now()
current_timestamp = str(current_timestamp)
time = current_timestamp.replace(' ','_').replace(".","_").replace("-","_").replace(":","_")

path = f'{log_catalog}.Upgrade_logs.permissions_upgrade_log_{user_email.split(".")[0]}_{time}'
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
df = spark.sql(f"Grant Use schema on schema {log_catalog}.upgrade_logs to `{user_email}`")
df = spark.sql(f"grant use catalog on catalog {log_catalog} to `{user_email}`")

# COMMAND ----------


