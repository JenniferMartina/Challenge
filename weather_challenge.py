import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,DoubleType,TimestampType,IntegerType
from pyspark.sql.functions import col,avg
from pyspark.sql.functions import *
from pyspark.sql.window import Window


#Set log path name and logger
path='log'
log_filename='weather.log'
lognm=os.path.join(path,log_filename)
logging.basicConfig(filename=lognm,
                            format='%(asctime)s:%(msecs)d :%(name)s :%(levelname)s: %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.INFO)

log=logging.getLogger()

#Create Sparksession
spark=SparkSession.builder.master('local[1]').appName('Weather').getOrCreate()


#Import global weather dataset

df_global=spark.read.option("header",True).option("inferSchema",True).csv('C:/Users/jenni/OneDrive/Documents/Challenge/paytmteam-de-weather-challenge/data/2019/*.csv')

count=df_global.count()
print("Count of records for global: ",count)
log.info('Global Weather data loaded')

#Import Station dataset

df_station=spark.read.option("header",True).option("inferSchema",True).csv('C:/Users/jenni/OneDrive/Documents/Challenge/paytmteam-de-weather-challenge/stationlist.csv')

count_st=df_station.count()
print("Count of records for station: ",count_st)
log.info('Station data loaded')

#Import Country dataset

df_cntry=spark.read.option("header",True).option("inferSchema",True).csv('C:/Users/jenni/OneDrive/Documents/Challenge/paytmteam-de-weather-challenge/countrylist.csv')

count_cntry=df_cntry.count()
print("Count of records for Country: ",count_cntry)
log.info('Country data loaded')


# Join Station with Country to get station number and full country name

df_st_cntry=df_station.alias('a').join(df_cntry.alias('b'),[col('a.COUNTRY_ABBR')==col('b.COUNTRY_ABBR')],how='inner').\
                       selectExpr('a.STN_NO','b.COUNTRY_FULL')

#check for duplicates in station dataset based on station number
df_station.createOrReplaceTempView('Station')
df_station_dups=spark.sql('select STN_NO,count(*) from Station group by STN_NO having count(*)>1')

#There are duplicates for 9 stations. Picking the first one from the records out of the duplicates based on alphabetical order as there is no explanantion on which country to pick.

windowspec=Window.partitionBy(col('STN_NO')).orderBy(col("COUNTRY_FULL"))
df_st_cntry_dedup=df_st_cntry.withColumn('rankno',row_number().over(windowspec)).filter(col('rankno')==1)

#Sample record before and after dedup
#df_st_cntry.where(col('STN_NO')=='785145').show()
#df_st_cntry_dedup.where(col('STN_NO')=='785145').show()

# Join above dataframe with global weather dataset

df_global_full_cntry=df_global.alias('a').join(df_st_cntry_dedup.alias('b'),[col('a.STN---')==col('b.STN_NO')],how='left').selectExpr('a.*','b.COUNTRY_FULL')

############################Step 2####################################

# Q1: Which country had the hottest avg mean temperature over the year?

#df_global_full_cntry.filter(col('TEMP')==9999.9).show()
#df_global_full_cntry.select(max(col('TEMP'))).show()

#Answer
df_global_full_cntry.groupBy(col('COUNTRY_FULL')).agg(avg(col('TEMP')).alias('mean_temp'))\
                    .orderBy(col('mean_temp'),ascending=False).head(1)
                    
#Answer: DJIBOUTI, 90.06
                    
# Q3: Which country had the second highest average mean wind speed over the year?

#df_global_full_cntry.select(max(col('WDSP'))).show()

df_q3=df_global_full_cntry.withColumn('wind_speed',when(col('WDSP')==999.9,None).otherwise(col('WDSP'))).groupBy(col('COUNTRY_FULL')).agg(avg(col('wind_speed')).alias('mean_wind'))\
                    .orderBy(col('mean_wind'),ascending=False)

windowspecq3=Window.orderBy(col("mean_wind").desc())
df_q3.withColumn('rankno',row_number().over(windowspecq3)).filter(col('rankno')==2)

#Answer:  ARUBA,15.975


