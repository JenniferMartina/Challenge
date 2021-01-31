import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,DoubleType,TimestampType,IntegerType
from pyspark.sql.functions import col,avg
from pyspark.sql.functions import *
from pyspark.sql.window import Window


try:
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


 ############################Step 2####################################

 def q1(df_global_full_cntry):

  # Q1: Which country had the hottest avg mean temperature over the year?

  #No default value(999.9) for the column TEMP

  df_q1=df_global_full_cntry.groupBy(col('COUNTRY_FULL')).agg(avg(col('TEMP')).alias('mean_temp'))\
                    .orderBy(col('mean_temp'),ascending=False)
                    
  #Answer: DJIBOUTI, 90.06
 
  windowspecq1=Window.orderBy(col("mean_temp").desc())
  df_final_1=df_q1.withColumn('rankno',dense_rank().over(windowspecq1)).filter(col('rankno')==1)

  # Writing the output for question1 csv file.
  df_final_1.coalesce(1).write.option('header',True).csv('q1_output.csv')
  log.info('Successfully completed the question 1 of the challenge')

 def q2(df_global_full_cntry):

  # Q2: Which country had the most consecutive days of tornadoes/funnel cloud formations?
 
  # Create a new column that indicates Tornado by taking the last letter of the column FRSHTT
  df_q2=df_global_full_cntry.withColumn('tornado_ind',substring(col('FRSHTT').cast('string'),-1,1))

  # There are multiple stations per country. So changing the garin of the data by taking the max of the tornado ind within each country and date. If any one of the stations within a country had a Tornado on a particular day then the tornado_ind_max is set to 1.
  df_q2_cntry=df_q2.groupBy(col('COUNTRY_FULL'),col('YEARMODA')).agg(max(col('tornado_ind')).alias('tornado_ind_max')).selectExpr('COUNTRY_FULL','YEARMODA','tornado_ind_max')


  # Logic to find out consecutive days with or without Tornado per country and day.
  # Step 1: Assign row number within each country ordered by date
  # Step 2: Assign row number within each country and tornado_ind_max indicator ordered by date
  # Step 3: Subtract row numbers in step 2 from step 1. This would help in partitioning further based on consecutive days.
  # Step 4: Remove days without Tornado from the dataframe
  # Step 5: Select countries where the number generated in step 3 is the highest.
  windowsinner=Window.partitionBy(col('COUNTRY_FULL'),col('tornado_ind_max')).orderBy(col("YEARMODA"))
  windowsouter=Window.partitionBy(col('COUNTRY_FULL')).orderBy(col("YEARMODA"))
  windowsfinal=Window.partitionBy(col('COUNTRY_FULL'),col('new_grp')).orderBy(col("YEARMODA"))

  df_q2_grp=df_q2_cntry.withColumn('new_grp',row_number().over(windowsouter)-row_number().over(windowsinner))\
               .filter(col('tornado_ind_max')==1)

  df_final=df_q2_grp.withColumn('consecutive_tornado_no',row_number().over(windowsfinal)).groupBy(col('COUNTRY_FULL')).\
                      agg(max(col('consecutive_tornado_no')).alias('max_tornado')).\
                      withColumn('rankval',dense_rank().over(Window.orderBy(col('max_tornado').desc()))).filter(col('rankval')==1)
                      
  # Writing the output for question2 csv file.
  df_final.coalesce(1).write.option('header',True).csv('q2_output.csv')
  log.info('Successfully completed the question 2 of the challenge')

 def q3(df_global_full_cntry):

  # Q3: Which country had the second highest average mean wind speed over the year?

  # Replace default value(999.9) for WDSP with None
  df_q3=df_global_full_cntry.withColumn('wind_speed',when(col('WDSP')==999.9,None).otherwise(col('WDSP'))).groupBy(col('COUNTRY_FULL')).agg(avg(col('wind_speed')).alias('mean_wind'))\
                    .orderBy(col('mean_wind'),ascending=False)
 
  windowspecq3=Window.orderBy(col("mean_wind").desc())
  df_final_3=df_q3.withColumn('rankno',dense_rank().over(windowspecq3)).filter(col('rankno')==2)

  #Answer:  ARUBA,15.975

  # Writing the output for question3 csv file.
  df_final_3.coalesce(1).write.option('header',True).csv('q3_output.csv')
  log.info('Successfully completed the question 3 of the challenge')

 def read_source():
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

  df_st_cntry=df_station.alias('a').join(df_cntry.alias('b'),[col('a.COUNTRY_ABBR')==col('b.COUNTRY_ABBR')],how='inner').selectExpr('a.STN_NO','b.COUNTRY_FULL')

  #check for duplicates in station dataset based on station number
  df_station.createOrReplaceTempView('Station')
  df_station_dups=spark.sql('select STN_NO,count(*) from Station group by STN_NO having count(*)>1')

  #There are duplicates for 9 stations. Picking the first one from the records out of the duplicates based on alphabetical order as there is no explanantion on which country to pick.

  windowspec=Window.partitionBy(col('STN_NO')).orderBy(col("COUNTRY_FULL"))
  df_st_cntry_dedup=df_st_cntry.withColumn('rankno',row_number().over(windowspec)).filter(col('rankno')==1)

  # Join above dataframe with global weather dataset

  df_global_full_cntry=df_global.alias('a').join(df_st_cntry_dedup.alias('b'),[col('a.STN---')==col('b.STN_NO')],how='left').selectExpr('a.*','b.COUNTRY_FULL')

  log.info('Processed dataframe has been loaded')
  return df_global_full_cntry

 #function call
 df_global_full_cntry=read_source()
 q1(df_global_full_cntry)
 q2(df_global_full_cntry)
 q3(df_global_full_cntry)

 log.info('Successfully completed the challenge')


except Exception as e:
    log.exception('Exception occured')



