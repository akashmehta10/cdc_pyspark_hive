import sys
import datetime, time
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from scd_lib import get_no_change_records, get_updated_records, get_new_records, get_deleted_rows

conf = SparkConf()
conf.set('set hive.vectorized.execution', 'true')
conf.set('set hive.vectorized.execution.enabled', 'true')
conf.set('set hive.cbo.enable', 'true')
conf.set('set hive.compute.query.using.stats', 'true')
conf.set('set hive.stats.fetch.column.stats','true')
conf.set('set hive.stats.fetch.partition.stats', 'true')
conf.set('spark.cleaner.referenceTracking.cleanCheckpoints', 'true')


spark = SparkSession.builder.appName("scd_driver_program").config(conf=conf).enableHiveSupport().getOrCreate()
spark.sql('set hive.exec.dynamic.partition=True')
spark.sql('set hive.exec.dynamic.partition.mode=nonstrict')

schema = StructType([
	StructField(name='eid', dataType= IntegerType(), nullable = False),
	StructField(name='name', dataType= StringType(), nullable = False),
	StructField(name='address', dataType= StringType(), nullable = False),
	StructField(name='phone_num', dataType= StringType(), nullable = False)
	])


#rec_eff_dt = datetime.datetime.fromtimestamp(time.time().strftime('%Y-%m-%d'))
rec_eff_dt = '2021-08-07'
# To Simulate
#rec_eff_dt = '2021-08-08'
key_list = ['eid']
ignored_columns = ['']

history = spark.sql(''' select * from scd_hive_db.employee ''')
current = spark.read.format('csv').option("header", False).schema(schema).load("/tmp/day0.csv")
# To Simulate
#current = spark.read.format('csv').option("header", False).schema(schema).load("/tmp/day1.csv")

if len(history.head(1)) == 0:
	# First time run, mark everything as an INSERT
	print("First time run started...")
	new_rows_df = get_new_records(history, current, key_list, rec_eff_dt)
	new_rows_df = new_rows_df.select(history.columns)
	print("Writing to History...")
	new_rows_df.write.mode("overwrite").insertInto("scd_hive_db.employee_history", overwrite=True)
	print("Data saved to History...")
	print("Writing to Snapshot...")
	new_rows_df.write.mode("overwrite").saveAsTable("scd_hive_db.cdc_employee_temp")
	new_rows_df_temp_table = spark.sql(''' select * from scd_hive_db.cdc_employee_temp ''')
	new_rows_df_temp_table.write.mode("overwrite").insertInto("scd_hive_db.employee", overwrite=True)
	print("First time run end...")
	print("Data saved to Snapshot...")
else:
	print("Subsequent run started...")
	updated_rows_df = get_updated_records(history, current, key_list, rec_eff_dt, ignored_columns)
	deleted_rows_df = get_deleted_rows(history, current, key_list, rec_eff_dt)
	new_rows_df = get_new_records(history, current, key_list, rec_eff_dt)
	unchanged_rows_df = get_no_change_records(history, current, key_list, ignored_columns)
	history_df = updated_rows_df.unionByName(deleted_rows_df).unionByName(new_rows_df)
	history_df = history_df.select(history.columns)
	print("Writing to History...")
	history_df.write.mode("overwrite").insertInto("scd_hive_db.employee_history", overwrite=True)
	print("Data saved to History...")
	unchanged_rows_df = unchanged_rows_df.select(history.columns)
	snapshot_df = history_df.filter("row_opern != 'D'").unionByName(unchanged_rows_df)
	print("Writing to Snapshot...")
	snapshot_df.write.mode("overwrite").saveAsTable("scd_hive_db.cdc_employee_temp")
	new_rows_df_temp_table = spark.sql(''' select * from scd_hive_db.cdc_employee_temp ''')
	new_rows_df_temp_table.write.mode("overwrite").insertInto("scd_hive_db.employee", overwrite=True)
	print("Data saved to Snapshot...")

print("Finished CDC Processing!")
