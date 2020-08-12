#!/usr/bin/env python
# coding: utf-8

# In[1]:


### Needed for running spark on Local computer

import findspark
findspark.init()


# In[2]:


# Importing pyspark and creating Sparkcontext and SparkSession

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime

spark = SparkSession.builder.appName('testing').getOrCreate()
sc = spark.sparkContext


# In[3]:


# Using SparkContext to import data lake file

trades = sc.textFile("datalake_file.txt")


# In[4]:


events = trades.map(lambda t: t.split("|") )

#### Question 1: 

flat_events = events.flatMap(lambda trade: [event for event in trade] )
flat_events.count()


# In[172]:


### Question 2:

mapTypes = lambda array: [ array[0], datetime.strptime(array[1], '%d%m%Y:%H:%M'), float(array[2]) ]

typed_flat_events = flat_events.flatMap(lambda event: [*map(mapTypes, [event.strip().split(",")])] )

typed_flat_events.map(lambda x: x[0]).distinct().collect()


# In[6]:


### Question 3 (Total amount for each product):

typed_flat_events.map(lambda lst: (lst[0], lst[-1])).reduceByKey(lambda price1, price2: price1 + price2).collect()


# In[285]:


### Question 4 (Sort events by time they occured)

typed_flat_events.map(lambda lst: (lst[0], lst[1], lst[2]) ).sortBy(lambda event: event[1]).collect()


# In[284]:


### Question 7 (Maximum price for each product):

typed_flat_events.map(lambda lst: (lst[0], lst[-1])).reduceByKey(lambda a, b: a if a>b else b).collect()


# In[9]:


### Question 8 (Minimum price for each product):

typed_flat_events.map(lambda lst: (lst[0], lst[-1])).reduceByKey(lambda a, b: a if a<b else b).collect()


# In[127]:


#trade_events = trades.map(lambda t: t.split("|") ).map(lambda lst: [[*map(mapTypes, [x.strip().split(",")])][0] for x in lst])
trades.map(lambda t: t.split("|") ).flatMap(lambda lst: [[*map(mapTypes, [x.strip().split(",")])] for x in lst])


# In[128]:


### Question 9 (Total amount for each trade)

#trade_events.map(lambda trade: [ x[2] for x in trade] ).collect()
trade_events.map(lambda trade: sum([ x[2] for x in trade]) ).collect()


# In[15]:


### Question 9 (Min price of for each trade)

trade_events.map(lambda trade: min([ x[2] for x in trade]) ).collect()


# In[296]:


### Question 9 (Max price for each trade)
trade_events.map(lambda trade: max([ x[2] for x in trade]) ).collect()


# In[297]:


### Question 5 (For a givent event get related events that make up the trade)

event = 'SPOT,29052016:12:30,11.23'
trades.filter(lambda lst: event in lst).map(lambda trade: trade.split("|")).map(lambda trades: [*filter(lambda k: event not in k, trades)] ).collect()


# In[176]:


###Question #6 (Data needs to be save on hive) 

### Google DataProc Hadoop Cluster ###

# File System

# wget https://raw.githubusercontent.com/sakksoftware/hsbc/master/trades.csv
# wget https://raw.githubusercontent.com/sakksoftware/hsbc/master/events.csv 

### Hadoop

# hadoop fs -mkdir /user/kwadwosakyi/trades
# hadoop fs -mkdir /user/kwadwosakyi/events
# hadoop fs -put trades.csv /user/kwadwosakyi/trades
# hadoop fs -put events.csv /user/kwadwosakyi/events

### Hive 
# hive
# create database if not exists hsbc_interview;
# show databases;
# use hsbc_interview;
# create external table events(product String, time_stamp timestamp, price float) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' location '/user/kwadwosakyi/events' ;
# create external table trades(trades String) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\n' location '/user/kwadwosakyi/trades' ;
# show tables;
# select * from events;
# select * from trades


# In[289]:


event_df = typed_flat_events.toDF(['product', 'timestamp', 'price'])


# In[290]:


event_df.show()


# In[291]:


### Writing event dataframe to csv file

event_df.coalesce(1).write.option("timestampFormat", "yyyy-MM-dd HH:mm:ss").format("csv").save("events")


# In[203]:


### Creating schema for trades which maintains structure of events in trade

### Importing types to be used to create schema and creating schema
from pyspark.sql.types import *
event_schema = StructType([
    StructField("product", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("price", FloatType())
])
trade_schema = ArrayType(event_schema)

### Creating dataframe from Schema
df_full = spark.createDataFrame(trade_events, trade_schema)


# In[204]:


# Displaying schema of trades with event structure
df_full.printSchema()


# In[294]:


# Example of selecting first trade
df_full.select('value').collect()[0]


# In[275]:


# Converting string of trades into list of trades for convenience
trades_list = trades.map(lambda trade: [trade])


# In[278]:


# Creating dataframe from list of trades
trades_df = trades_list.toDF(['trades'])


# In[295]:


# Writing list of trades to csv file
trades_df.coalesce(1).write.format("csv").save("trades")

