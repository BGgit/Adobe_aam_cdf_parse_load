

import pandas as pd
import numpy as np

import datetime
import sys
import re
from itertools import groupby, islice, takewhile
import gzip
import sqlalchemy
import cx_Oracle
import boto3



# look up seaborn


# date based variables
format = '%y_%m_%d'

TODAY = datetime.date.today()
add = datetime.timedelta(days=1)
yesterday = datetime.date.today() - add
dd = datetime.date.strftime(TODAY,format)

# conection variables
connection = cx_Oracle.connect("crm/Prd@dwdbd1.darden.com:1521/crmprd")
cur = connection.cursor()

# datafrae headers
columns = ['EVENT_TIME','DEVICE','CONTAINER_ID','REALIZED_TRAITS','REALIZED_SEGMENTS','REQUEST_PARAMETERS','REFERER','IP_ADDRESS','MID','ALL_SEGMENTS','ALL_TRAITS']


# Opperations and Transform

# -- >> test and sample the file
aam_df = pd.read_csv(gzip.open(r'C:\Users\gembxg9\Downloads\AAM_CDF_3526_000000_0.gz') , sep = '\x01', header  = None, names = columns)
# print(aam_df.head(10))

# prepdata from zip
# add record_id for values
aam_df_idx = aam_df.index.values +1
aam_df.insert(0,column = 'RECORD_ID' , value=aam_df_idx)
# -->> get a sample of test data for insert
aam_df_clean = aam_df.fillna('NA')
# end prep data from zip


# create test data set
# aam_df_test_dataset = aam_df_clean.head(100)
# print(aam_df_test_dataset)
# end test dataset

# SQL Drop
drop_table = """DROP TABLE BG_AAM_TESTCDF_LOAD"""


# SQL Create Oracle Table
create_table_1 = """
CREATE TABLE BG_AAM_TESTCDF_LOAD
    (
        RECORD_ID INT
        ,EVENT_TIME VARCHAR2(50)
        ,DEVICE VARCHAR2(50)
        ,CONTAINER_ID VARCHAR2(50)
        ,REALIZED_TRAITS VARCHAR2(512)
        ,REALIZED_SEGMENTS VARCHAR2(100)
        ,REQUEST_PARAMETERS CLOB
        ,REFERER VARCHAR2(1000)
        ,IP_ADDRESS VARCHAR2(20)
        ,MID VARCHAR2(50)
        ,ALL_SEGMENTS VARCHAR2(512)
        ,ALL_TRAITS VARCHAR2(1000)
    )
"""

insert_statement = """
INSERT INTO BG_AAM_TESTCDF_LOAD
    (
        RECORD_ID
        ,EVENT_TIME
        ,DEVICE
        ,CONTAINER_ID
        ,REALIZED_TRAITS
        ,REALIZED_SEGMENTS
        ,REQUEST_PARAMETERS
        ,REFERER
        ,IP_ADDRESS
        ,MID
        ,ALL_SEGMENTS
        ,ALL_TRAITS
    )
    values (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12)
"""
try:
    cur.execute(create_table_1)
    connection.commit()
except:
    cur.execute(drop_table)
    connection.commit()
    cur.execute(create_table_1)
    connection.commit()


try:
    cur.prepare(insert_statement)
except cx_Oracle.DatabaseError as Exception:
    printf('Failed to prepare insert cursor')
    printException(Exception)
    exit(1)

# getting record count of the dataframe
aam_df_dataset_lst_check = aam_df_clean.values.tolist()
print(len(aam_df_dataset_lst_check))

batch_size = 50000
# i = 0
# j = 1

def chunker(seq,size):
    return(seq[pos:pos+size] for pos in range(0,len(seq),size))


for i in chunker(aam_df_clean,batch_size):
    ### Insert and Itterate to inset records
    aam_df_dataset_lst = i._csv.tolist()
    # batchchunk.index += j
    # i += 1

    # print ( aam_df_dataset_lst )


    # use for memory management

    # control number of records to bind for insert
    # cur.bindarraysize = 102000000
    # cur.setinputsizes(int,50,50,50,50,50,4000,1000,20,50,50,1000)
    # cur.arraysize = 50000
    cur.executemany(insert_statement,aam_df_dataset_lst)
    # j = batchchunk.index[1] + 1
    # cur.fetchall()
    connection.commit()
    number_of_records_loaded = cur.execute("""SELECT COUNT(*), SYSDATE FROM BG_AAM_TESTCDF_LOAD GROUP BY SYSDATE""")
    record_out = cur.fetchall()
    for row in record_out:
        print(row)
connection.close()


