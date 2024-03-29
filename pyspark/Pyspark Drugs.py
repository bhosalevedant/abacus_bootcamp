# Databricks notebook source
from pyspark.sql import functions as f

# COMMAND ----------

drugs_product = spark.read.format("csv").option("header",True).load("dbfs:/FileStore/tables/Drugs_product.csv")
drugs_product.show()

# COMMAND ----------

drugs_product.printSchema()

# COMMAND ----------

drugs_package = spark.read.format("csv").option("header",True).load("dbfs:/FileStore/tables/Drugs_package.csv")
drugs_package.show()

# COMMAND ----------

drugs_package.printSchema()

# COMMAND ----------

display(drugs_package)

# COMMAND ----------

display(drugs_product)

# COMMAND ----------

drugs_product.printSchema()

# COMMAND ----------

drugs_product.columns

# COMMAND ----------

headers_product = ['PRODUCT_ID',
 'PRODUCT_NDC',
 'PRODUCT_TYPE_NAME',
 'PROPRIETARY_NAME',
 'PROPRIETARY_NAME_SUFFIX',
 'NONPROPRIETARY_NAME',
 'DOSAGE_FORM_NAME',
 'ROUTE_NAME',
 'START_MARKETING_DATE',
 'END_MARKETING_DATE',
 'MARKETING_CATEGORY_NAME',
 'APPLICATION_NUMBER',
 'LABELER_NAME',
 'SUBSTANCE_NAME',
 'ACTIVE_NUMERATOR_STRENGTH',
 'ACTIVE_INGRED_UNIT',
 'PHARM_CLASSES',
 'DEA_SCHEDULE']

# COMMAND ----------

head_product = [x.lower() for x in headers_product]

# COMMAND ----------

head_product

# COMMAND ----------

header_dict = dict(zip(drugs_product.columns,head_product))
header_dict

# COMMAND ----------

for old, new in header_dict.items():
    print(old,new)

# COMMAND ----------

drugs_product_renamed = drugs_product
for old, new in header_dict.items():
    drugs_product_renamed = drugs_product_renamed.withColumnRenamed(old,new)


# COMMAND ----------

#drugs_product_renamed = drugs_product.withColumnRenamed(header_dict)
display(drugs_product_renamed)

# COMMAND ----------

drugs_product_renamed.columns

# COMMAND ----------

drugs_product_dateformat = drugs_product_renamed.withColumns(
    {
    'start_marketing_date':f.to_date('start_marketing_date',"yyyyMMdd"),
    'end_marketing_date':f.to_date('end_marketing_date',"yyyyMMdd")
    }
)  

# COMMAND ----------

display(drugs_product_dateformat)

# COMMAND ----------

drugs_package.columns

# COMMAND ----------

package_header =  ['PRODUCT_ID', 'PRODUCT_NDC', 'NDC_PACKAGE_CODE', 'PACKAGE_DESCRIPTION']

# COMMAND ----------

head_package = [x.lower() for x in package_header]

# COMMAND ----------

head_package

# COMMAND ----------

package_header_dict = dict(zip(drugs_package.columns,head_package))
package_header_dict

# COMMAND ----------

drugs_package_renamed = drugs_package
for old, new in header_dict.items():
    drugs_package_renamed = drugs_package_renamed.withColumnRenamed(old,new)


# COMMAND ----------

display(drugs_package_renamed)

# COMMAND ----------

joined_df = drugs_product_dateformat.join(drugs_package_renamed,'product_ndc','left')
display(joined_df)

# COMMAND ----------

product_type_count = joined_df.groupBy('product_type_name').count()
display(product_type_count)

# COMMAND ----------

product_routes = joined_df.groupBy('route_name').count()
display(product_routes)

# COMMAND ----------


