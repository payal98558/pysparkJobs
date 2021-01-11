import sys
import os
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Pyspark job") \
        .getOrCreate()

    emp_df = spark.read.csv("file:\\C:\projects\pysparkJobs\spark_emp_rdd_df_ds_data.csv", header=True, inferSchema=True)
    dept_df= spark.read.csv("file:\\C:\projects\pysparkJobs\spark_dept_rdd_df_ds_data.csv", header=True, inferSchema=True)
    emp_df.show()
    #Record with maximum salary
    print("Record of employee with maximum salary")
    max_emp_df = emp_df.filter(emp_df.Sal == emp_df.agg({"Sal": "max"}).collect()[0][0]).select('FName', 'Lname', 'Sal').show()

    #dept_df.show()

    deptSalary_df = emp_df.join(dept_df, on='DeptNo', how='inner').groupBy("DeptName").sum("Sal").withColumnRenamed("sum(Sal)", "DeptSal")

    #Department with the highest salary
    print("Department with the highest Salary")
    max_sal_dept_df = deptSalary_df.filter(deptSalary_df.DeptSal == deptSalary_df.agg({"DeptSal": "max"}).collect()[0][0]).select('DeptName', 'DeptSal').show()
