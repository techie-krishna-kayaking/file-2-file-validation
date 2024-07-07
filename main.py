'''
PYSPARK code for ETL Testing Automation
============================================


BELOW ARE THE PIP INSTALL COMMANDS BEFORE USING THE BELOW CODE
--------------
pip install python
pip3 install pyspark
pip3 install psutil
pip3 install requests
pip3 install pandas
'''

# Input file paths - /Users/kkrishna/downloads/data/employees.csv


from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession.builder.appName("ES").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


def handle_empty_values(df):
    """
    Function to handle empty values in all columns of a DataFrame.
    Replaces empty strings with None values.

    Parameters:
        df (DataFrame): Input DataFrame.

    Returns:
        DataFrame: DataFrame with empty values replaced by None.
    """
    for column in df.columns:
        # Replace empty strings with None values for each column
        df = df.withColumn(column, F.when(F.col(column) == "", None).otherwise(F.col(column)))
    return df


def get_primary_key():
    primary_key = input("Enter the primary key column name: ")
    return primary_key

def readcsv():
    filepath = input("Enter your CSV file path: ")
    df = spark.read.option("header", "true").csv(filepath)
    df = handle_empty_values(df)
    return df

def readjson():
    filepath = input("Enter your JSON file path: ")
    df = spark.read.format("json").option("multiline", "true").load(filepath)
    df = handle_empty_values(df)
    return df

def readparquet():
    filepath = input("Enter your PARQUET file path: ")
    df = spark.read.format("parquet").option("header", "true").load(filepath)
    df = handle_empty_values(df)
    return df

def readexcel():
    filepath = input("Enter your EXCEL file path: ")
    df = pd.read_excel(filepath)
    df = handle_empty_values(df)
    return df

def csv2csv():
    srcdf = readcsv()
    tgtdf = readcsv()
    comparedf(srcdf, tgtdf)

def csv2json():
    srcdf = readcsv()
    tgtdf = readjson()
    comparedf(srcdf, tgtdf)

def csv2parquet():
    srcdf = readcsv()
    tgtdf = readparquet()
    comparedf(srcdf, tgtdf)

def comparedf(src, tgt):
    src_distinct = src.distinct()
    tgt_distinct = tgt.distinct()

    print("\n\n=============== SHOWING DFS ================ ")
    print("\n\n=== SOURCE DATA ===")
    src.show()
    print("\n\n=== TARGET DATA ===")
    tgt.show()

    print("\n\n=============== TESTING STARTED ================ ")

    if src_distinct.count() == src.count() and tgt_distinct.count() == tgt.count():
        print("Record count - PASS")
    else:
        print("Record count - FAIL")

    # NULL CHECK TEST CASE
    nullcnt = tgt.filter(tgt["emp_no"].isNull() | tgt["FIRST_NAME"].isNull()).count()

    if nullcnt == 0:
        print("NULL Check - PASS")
    else:
        print("NULL Check - FAIL")

    # DUPLICATE CHECK TEST CASE
    dupcnt = tgt.groupBy(primary_key).count().filter("count > 1").count()

    if dupcnt == 0:
        print("DUPLICATE Check - PASS")
    else:
        print("DUPLICATE Check - FAIL")

    # MINUS TEST CASE
    diff_df = src.exceptAll(tgt)

    if diff_df.isEmpty():
        print("Source MINUS Target - PASS")
    else:
        print("Source MINUS Target - FAIL")

while True:
    ch = int(input('''\n\n\n MAIN PAGE - AUTOMATION TESTING \n Choose the option below to start the testing :
        1. File to File comparison - CSV to CSV
        2. File to File comparison - CSV to JSON
        3. File to File comparison - CSV to PARQUET
        4. File to File comparison - CSV to PARQUET
        5. File to File comparison - CSV to EXCEL
        6. File to File comparison - DB to DB
        7. Enter 0 to EXIT - '''))
    if ch == 0:
        exit()

    primary_key = get_primary_key()

    if ch == 1:
        csv2csv()
    elif ch == 2:
        csv2json()
    elif ch == 3:
        csv2parquet()
    elif ch == 4:
        csv2excel()
    elif ch == 5:
        db2db()
    elif ch == 6:
        db2csv()
    else:
        print("WRONG ENTRY !")
