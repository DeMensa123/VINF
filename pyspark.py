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

import whoosh_search as search

# to run the file use this commmand:
# spark-submit pyspark.py <master> <input_file> <number_of_partitions> <ouput_directory>
# master - master URL for the cluster
# input_file - freebase input file
# number_of_partitions - number of partitions (output files)
# name of output directory

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


# regex for names, aliases, dates of birth and dates of death
re_name = '(\/[gm]\..+\t<http:\/\/rdf\.freebase\.com\/ns\/type\.object\.name>\t\".*\"@en)'
re_alias = '(\/[gm]\..+\t<http:\/\/rdf\.freebase\.com\/ns\/common\.topic\.alias>\t\".*\"@en)'
re_birth = '(\/[gm]\..+\t<http:\/\/rdf\.freebase\.com\/ns\/people\.person\.date_of_birth>\t)'
re_death = '(\/[gm]\..+\t<http:\/\/rdf\.freebase\.com\/ns\/people\.deceased_person\.date_of_death>\t)'

# define SparkSession
try: 
    spark = SparkSession.builder \
        .master(sys.argv[1]) \
        .appName("FreebasePeople") \
        .getOrCreate() 
    
except Exception as e:
    print(e)
    print("Nezadali ste mastra!")

# filter input file
try:
    file = sys.argv[2]
    freebase = spark.sparkContext.textFile(file)

    people = freebase \
        .filter(lambda x: re.search(re_name,x) or re.search(re_alias,x) or re.search(re_birth,x) or re.search(re_death,x)) \
        .distinct() \
        .map(lambda x: re.sub('(http\:\/\/rdf.freebase.com\/ns\/)|(\^\^.*\.)|(\@.*\.)|\<|\>|\"',"",x)) \
        .map(lambda x: x.split('\t'))
except:
    print("Nezadali ste vstupny subor!")

# define schema for dataframes
schema = StructType([StructField('subject', StringType(), True),
                    StructField('predicate', StringType(), True),
                    StructField('object', StringType(), True, metadata = {"maxlength":2048})])

# create dataframes 
try:
    names = spark.createDataFrame(people.filter(lambda x: "type.object.name" in x[1]), schema)
    aliases = spark.createDataFrame(people.filter(lambda x: "common.topic.alias" in x[1]), schema)
    births = spark.createDataFrame(people.filter(lambda x: "people.person.date_of_birth" in x[1]), schema)
    deaths = spark.createDataFrame(people.filter(lambda x: "people.deceased_person.date_of_death" in x[1]), schema)

    births = births.withColumn("note", f.lit("OK"))
    deaths = deaths.withColumn("note", f.lit("OK")) 

    names.registerTempTable("names")
    aliases.registerTempTable("aliases")
    births.registerTempTable("births")
    deaths.registerTempTable("deaths")

    sql_context = SQLContext(spark.sparkContext)

    sql = sql_context.sql("""
    select names.object as name, 
    ifnull(aliases.object, '-') as alias,
    ifnull(cast(births.object as date), (cast(deaths.object as date) - 100*365) ) as birth,
    ifnull(cast(deaths.object as date), (cast(births.object as date) + 100*365) ) as death,
    ifnull(births.note, 'incorrect') as b_note,
    ifnull(deaths.note, 'incorrect') as d_note
    from names
    left join births on names.subject = births.subject
    left join deaths on names.subject = deaths.subject
    left join aliases on names.subject = aliases.subject
    where births.object is not null or deaths.object is not null
    """)

except Exception as e:
    print(e)


# split data into n partitions and execute computations on the partitions in parallel
try:
    n = int(sys.argv[3])
    output_dir = sys.argv[4]

    sql.repartition(n).write.format('com.databricks.spark.csv') \
        .option("mapreduce.fileoutputcommitter.marksuccessfuljobs","false").save(output_dir, header = 'true')

    # rename output files to more readable form
    rename_dir(output_dir) 
except:
    print("Nezadali ste pocet vystupnych suborov alebo vystupny adresar")
