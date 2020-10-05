# coding=utf-8
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as sf
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

spark = SparkSession \
    .builder \
    .appName("Individual_Assignment") \
    .getOrCreate()

# DataFrame creation from JSON file
df = spark.read.json("data/purchases.json")

# Basic Operations
df.printSchema()
df.describe().show()
df.show(2)
print "Num of records:", df.count()

print ""
print "Answers: Rosalia Contreras Moreira"
print ""

# 1. Top 10 most purchased products
print "1. Top 10 most purchased products:"
df.groupBy(df.product_id, df.item_type).agg(sf.count(df.product_id).alias("top_items")).orderBy("top_items",
                                                                                                ascending=False).show(
    10)

# Top 10 by product_id
print "Top 10 by product_id:"
df.groupBy(df.product_id).agg(sf.count(df.product_id).alias("top_items")).orderBy("top_items",
                                                                                  ascending=False).show(10)

# Top by item_type
print "Top by item_type:"
df.groupBy(df.item_type).agg(sf.count(df.item_type).alias("top_item_type")).orderBy("top_item_type",
                                                                                    ascending=False).show()

# 2. Purchase percentage of each product type (item_type)
print "2. Purchase percentage of each product type:"
df.groupBy(df.item_type).agg((sf.count(df.item_type) / df.count() * 100).alias("percentage_items")).orderBy(
    "percentage_items", ascending=False).show()

# 3. Shop that has sold more products
print "3. Shop that has sold more products:"
df.groupBy(df.shop_id).agg(sf.count(df.shop_id).alias("Rank_shops")).orderBy("Rank_shops", ascending=False).show(
    1)

# 4. Shop that has billed more money
print "4. Shop that has billed more money:"
df.groupBy(df.shop_id).agg(sf.sum(df.price).alias("Top_Sales")).orderBy("Top_Sales", ascending=False).show(1)

# 5. Divide world into 5 geographical areas based in longitude (location.lon)
# and add a column with geographical area name, for example “area1”, “area2”, …(longitude values are not important)
print "location.lon describe:"
df.select(df.location.lon).describe().show()

# +-------+------------------+
# |summary|      location.lon|
# +-------+------------------+
# |  count|              3319|
# |   mean| 2.229834136788185|
# | stddev|102.88888106553975|
# |    min|         -179.8282|
# |    max|           179.937|
# +-------+------------------+

print "Total range of values 179.8282 + 179.937 = 359.7652 / 5 = 71.95304 (size of each bucket)"
print "area1: less than -107.87516"
print "area2: -107.87517 : -35.92212"
print "area3: -35.92213 : 36.03092"
print "area4: 36.03092 : 107.98396"
print "area5: more than 107.98397"


def location_to_areas(value):
    if value < -107.87516:
        return 'area1'
    elif -107.87517 <= value <= -35.92212:
        return 'area2'
    elif -35.92213 <= value <= 36.03092:
        return 'area3'
    elif 36.03092 <= value <= 107.98396:
        return 'area4'
    else:
        return 'area5'


print ""
print "5. New table with areas:"

udf_location_to_areas = udf(location_to_areas, StringType())
df_areas = df.withColumn("areas", udf_location_to_areas(df.location.lon))
df_areas.show()

# a. In which area is PayPal most used
print "Areas with the most payments done by paypal:"
df_areas.filter(df_areas.payment_type == "paypal").groupBy(df_areas.areas).agg(
    sf.count(df_areas.areas).alias("Paypal_by_areas")) \
    .orderBy("Paypal_by_areas", ascending=False).show(1)

# b. Top 3 most purchased products in each area
print "Top purchased products by area (total):"
df_areas.groupBy(df_areas.areas, df_areas.product_id, df_areas.item_type).agg(sf.count(df_areas.product_id). \
                                                                              alias("top_items_by_area")).orderBy(
    "top_items_by_area", ascending=False).show()

temporal = df_areas.groupBy(df_areas.areas, df_areas.product_id, df_areas.item_type).agg(sf.count(df_areas.product_id). \
                                                                                         alias(
    "top_items_by_area")).orderBy(df_areas.areas, "top_items_by_area", ascending=False)

print "Top 3 most purchased products in each area:"
temporal.withColumn("top_products", sf.row_number().over(Window.partitionBy("areas"). \
                                                         orderBy(temporal['top_items_by_area'].desc()))).filter(
    sf.col('top_products') <= 3).show()

# c. Area that has billed less money
print "Least sales by areas:"
df_areas.groupBy(df_areas.areas).agg(sf.sum(df_areas.price). \
                                     alias("Top_Sales")).orderBy("Top_Sales", ascending=True).show()

# 6. Products that do not have enough stock for purchases made
print "Read the stock.csv file:"
print ""

# DataFrame creation from JSON file
df_stock = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .csv("data/stock.csv")

# Basic Operations
df_stock.printSchema()
df_stock.describe().show()
print "Num of records:", df_stock.count()

print ""
print "6. Products that do not have enough stock for purchases made:"
df.select("product_id").groupBy("product_id").agg(sf.count("product_id").alias("num_sold")). \
    join(df_stock, "product_id").filter(sf.col('num_sold') > sf.col('quantity')).show()
