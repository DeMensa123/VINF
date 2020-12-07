#!/usr/bin/env python3

import findspark
findspark.init()
import pyspark
from pyspark import SparkContext
from pyspark import SparkConf
import pyspark.sql
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import functions as f
import re
import datetime
import sys
from os import listdir
import os 
from os.path import isfile, join


# to run the program use this commmand:
# spark-submit pyspark.py <mode> <input_file> <number_of_partitions> <ouput_directory> <master> <spark_executor_uri>
# mode - [1/2] 1 for local mode and 2 for cluster mode
# input_file - freebase input file
# number_of_partitions - number of partitions (output files)
# ouput_directory - name of output directory
# master - master URL for the cluster
# spark_executor_uri - only if you want to run spark on cluster

def rename_dir(readPath):
    writePath = readPath + '_csv'
    os.mkdir(writePath)

    file_list = [f for f in listdir(readPath)]

    for i in file_list:
        filename, file_extension = os.path.splitext(i)
        reg = '(\.[\w]+-[\w]+)|([\w]+-[\w]+)'
        result = re.match(reg, filename).group()
        if file_extension == '.csv':
            filename = filename.split('-')[1]
            os.rename(readPath + '/' + i, writePath + "/" + result + file_extension)


# regex for names, aliases and people
re_name = '(\/[gm]\..+\t<http:\/\/rdf\.freebase\.com\/ns\/type\.object\.name>\t\".*\"@en)'
re_alias = '(\/[gm]\..+\t<http:\/\/rdf\.freebase\.com\/ns\/common\.topic\.alias>\t\".*\"@en)'
re_person = '(\/[gm]\..+\t<http:\/\/rdf\.freebase\.com\/ns\/people\.person\..*>\t)'
re_dec_person = '(\/[gm]\..+\t<http:\/\/rdf\.freebase\.com\/ns\/people\.deceased_person\..*>\t)'

# for red colored text
CRED = '\033[91m'
CEND = '\033[0m'

if len(sys.argv) < 6:
    print(CRED + "Nezadali ste niektory z argumentov" + CEND)
    exit()
  
mode = sys.argv[1]
input_file = sys.argv[2]
n = int(sys.argv[3])
output_dir = sys.argv[4]
master = sys.argv[5]


# define SparkSession
try: 
    if mode == '1':
        spark = SparkSession.builder \
            .master(master) \
            .appName("FreebasePeople") \
            .getOrCreate() 

    elif mode == '2':
        spark = SparkSession.builder \
            .master(master) \
            .appName("FreebasePeople") \
            .getOrCreate() \
            .config("spark.executor.uri", sys.argv[6])
    else:
        print(CRED + "Zadajte mod v ktorom chcete spustit program: 1 (local) / 2 (cluster)" + CEND)
        exit()

except Exception as e:
    print(e)
    exit()

# filter input file
try:
    freebase = spark.sparkContext.textFile(input_file)

    filtered_data = freebase \
        .filter(lambda x: re.search(re_name,x) or re.search(re_alias,x) or re.search(re_person,x) or re.search(re_dec_person,x)) \
        .distinct() \
        .map(lambda x: re.sub('(http\:\/\/rdf.freebase.com\/ns\/)|(\^\^.*\.)|(\@.*\.)|\<|\>|\"|(\t\.)',"",x)) \
        .map(lambda x: x.split('\t')) 
except:
    print(CRED + "Zadali ste zly vstupny subor!" + CEND)
    exit()

# define schema for dataframes
schema = StructType([StructField('subject', StringType(), True),
                    StructField('predicate', StringType(), True),
                    StructField('object', StringType(), True, metadata = {"maxlength":2048})])

# create dataframes 
try:
    names = spark.createDataFrame(filtered_data.filter(lambda x: "type.object.name" in x[1]), schema)
    aliases = spark.createDataFrame(filtered_data.filter(lambda x: "common.topic.alias" in x[1]), schema)
    births = spark.createDataFrame(filtered_data.filter(lambda x: "people.person.date_of_birth" in x[1]), schema)
    deaths = spark.createDataFrame(filtered_data.filter(lambda x: "people.deceased_person.date_of_death" in x[1]), schema)
    others = spark.createDataFrame(filtered_data.filter(lambda x: "people" in x[1] and "date_of_birth" not in x[1] and "date_of_death" not in x[1]), schema)
    
    births = births.withColumn("note", f.lit(""))
    deaths = deaths.withColumn("note", f.lit(""))

    names.registerTempTable("names")
    aliases.registerTempTable("aliases")
    births.registerTempTable("births")
    deaths.registerTempTable("deaths")
    others.registerTempTable("others")

    sql_context = SQLContext(spark.sparkContext)

    people = sql_context.sql("""
        select names.subject as id, names.object as name, 
        ifnull(aliases.object, '-') as alias,
        case
            when births.object is not null then (cast(births.object as date)) 
            when deaths.object is not null and births.object is null then (cast(deaths.object as date) - 100*365)
            when deaths.object is null and births.object is null then '-'
        end as birth,
        case
            when deaths.object is not null then (cast(deaths.object as date))
            when births.object is not null and deaths.object is null then (cast(births.object as date) + 100*365)
            when deaths.object is null and births.object is null then '-'
        end as death,
        ifnull(births.note, 'Datum narodenia nemusi byt spravny.') as b_note,
        ifnull(deaths.note, 'Datum umrtia nemusi byt spravny.') as d_note
        from names
        left join aliases on names.subject = aliases.subject
        left join births on names.subject = births.subject
        left join deaths on names.subject = deaths.subject
        left join others on names.subject = others.subject
        where births.object is not null or deaths.object is not null or others.object is not null
        """)

    people = people.distinct()

except Exception as e:
    print(e)
    exit()


# split data into n partitions and execute computations on the partitions in parallel
try:
    people.repartition(n).write.format('com.databricks.spark.csv') \
        .option("mapreduce.fileoutputcommitter.marksuccessfuljobs","false").save(output_dir, header = 'true')
except Exception as e:
    print(CRED + "Chyba pri vystupnych suboroch" + CEND)
    exit(e)

# rename output files to more readable form
rename_dir(output_dir) 
