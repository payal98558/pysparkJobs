import sys
import os
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Pyspark job") \
        .getOrCreate()
    #sc = SparkContext('local', 'Spark SQL')
    emp_df = spark.read.csv("file:\\C:\projects\pysparkJobs\spark_emp_rdd_df_ds_data.csv", header=True, inferSchema=True)
    dept_df= spark.read.csv("file:\\C:\projects\pysparkJobs\spark_dept_rdd_df_ds_data.csv", header=True, inferSchema=True)
    emp_df.createOrReplaceTempView("EMP_RECORD")

    maxSalEmpRecord = spark.sql('SELECT FName, Lname, MAX(Sal) as Sal from EMP_RECORD order by Sal desc')
    print(maxSalEmpRecord)
    #print(spark.sql('select * from emp_df').show())
    print(dept_df.createOrReplaceTempView("DEPT_RECORD"))
    maxSalDept = spark.sql('SELECT dr.DeptName, sum(er.Sal) from DEPT_RECORD dr join EMP_RECORD er on er.DeptNo = dr.DeptNo groupby dr.DeptName, sum(Sal)')
    maxSal=spark.sql('select DeptName, Sal from DEPT_RECORD').groupBy('DeptName')
    print(maxSalDept)
