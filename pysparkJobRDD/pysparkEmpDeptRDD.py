import sys
import os
from pyspark import SparkContext, SparkConf


if __name__ == "__main__":
    # create Spark context with necessary configuration
    sc = SparkContext("local", "PySpark Word Count Example")

    # read data from csv file and split each line into words
    emp_csv= sc.textFile("file:\\C:\projects\pysparkJobs\spark_emp_rdd_df_ds_data.csv", use_unicode=True)
    print(emp_csv.collect())
    emp_rdd = sc.textFile("file:\\C:\projects\pysparkJobs\spark_emp_rdd_df_ds_data.csv", use_unicode=True).map(lambda line: line.split(","))\
        #.map(lambda line: (line[0], line[1]))
    dept_rdd = sc.textFile("file:\\C:\projects\pysparkJobs\spark_dept_rdd_df_ds_data.csv", use_unicode=True).map(lambda line: line.split(","))\
        #.map(lambda line: [line[i] for i in line])
    # count the occurrence of each word
    print(emp_rdd.collect())
    print(dept_rdd.collect())
    emp_rdd.max()
