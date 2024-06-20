# Databricks notebook source
# MAGIC %md
# MAGIC Catalog_table - this is a table that holds metadata details for the schema and data objects within the schema.
# MAGIC
# MAGIC the input we require is the fully qualified tablename of the catalog table, catalog name on UC to which the sata is to be placed, and user email:
# MAGIC
# MAGIC the catalog table would look something like this shown below:
# MAGIC
# MAGIC ###SCHEMANAME	              TABLENAME
# MAGIC ###unity_catalog_upgrade_1 	   Table_01
# MAGIC ###unity_catalog_upgrade_1	   Table_02

# COMMAND ----------

# Getting the fully qualified table name of the catalog table, and catalog name to which schemas would be created.
InputTableName = dbutils.widgets.get('fully_qualified_tablename_of_the_catalog_table').strip()
Catalog =  dbutils.widgets.get('UC_Catalog_name').strip()

# COMMAND ----------

# DBTITLE 1,This code block selects the schema names from the catalog table
#  Checking for the schemas already present in the catalog name provided.
present_schemas = spark.sql(f'show schemas in {Catalog}')
present_schemas = [row['databaseName'] for row in present_schemas.collect()]
# print(present_schemas)

#  Fetching the schemas from the catalog table.
InputTable = spark.sql(f"SELECT * from {InputTableName}")
# tables = [row['TABLENAME'] for row in InputTable.collect()]
schemas = [row['SCHEMANAME'] for row in InputTable.collect()]
# print(schemas)

#  Picking the unique schema names that need to be created.
unique_values = list(set(schemas))
result = [value for value in unique_values if value not in present_schemas]
print('schemas to create ',result)



# COMMAND ----------

# DBTITLE 1,Code block to create the required schema

def create_schema(arr):
    """
    Creates a schema in Unity Catalog using an AWS S3 location.

    Args:
        arr (str): The name of the schema to be created.

    Returns:
        None: Prints messages indicating whether the schema was successfully created or encountered an error.
    """
    try:
        a =spark.sql(f'desc schema {arr}')
        # print(a.collect())
        for value in a.collect():
            if(value['database_description_item'] =='Location'):
                loca = value['database_description_value']
        
        if( not (loca.startswith('s3'))):
            # print(loca, arr)
            raise Exception('schema :', arr, ' not present in an s3 location. Location: ',loca )
        q = spark.sql(f"create schema {Catalog}.{arr} managed location '{loca}'")
        print(arr ," schema created")
    except Exception as err:
        errr =str(err)
        errr =errr[:200]
        print(arr ," schema not created because of error :", errr)
    

for val in result:
    # print(val)
    create_schema(val)
    



# COMMAND ----------


