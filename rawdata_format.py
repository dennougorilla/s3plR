# coding: utf-8
#dennougorilla

#def lambda_handler(event, context):
#import {{{
import boto3
import pandas as pd
from tqdm import tqdm
<<<<<<< HEAD
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import datetime, date, timedelta
=======
import os
import pandas_redshift as pr

>>>>>>> 2dc71c8b4edbec3373c9b9bc9f781ba005e0c469
#}}}

s3 = boto3.client('s3')

#Make usecols {{{
colss_li = [i for i in range(0,211)]
del colss_li[11:21]
colss_li.remove(2)
colss_li.remove(7)
#}}}


bucket_name = 'ld-rawdata'
file_name = 'TR_JISSEKI/20161115XXXXXX/TR_JISSEKI_20161115232207.csv'#TODO
day = file_name[37:45] #day string
reader = pd.read_csv('s3n://'+bucket_name+'/'+file_name,
        encoding="cp932",
        header=None,
        #nrows=10,
        iterator=True,
        chunksize=1000,
        usecols=colss_li)
df = pd.concat((r for r in tqdm(reader)), ignore_index=True)


li = []
df = df[df[0].isin([day])]
hour = 7
base = df.loc[:, 0:10]

#Make hours list
for i in tqdm(range(19)):
    if hour < 24:
        base.loc[:, 0] = pd.datetime(int(day[0:4]),int(day[4:6]),int(day[6:8]), hour)
    elif hour > 23:
        base.loc[:, 0] = pd.datetime(int(day[0:4]),int(day[4:6]),int(day[6:8]), hour-24)+timedelta(days=1)
        
    hour+=1
    li.append(pd.concat([base, df.loc[:, 21+i*10:30+i*10]], axis=1))

#set columns
for i in tqdm(range(len(li))):
    li[i].columns = [j for j in range(19)]

##difference
#for i in tqdm(reversed(range(len(li)))):
#    if i != 0:
#        li[i][9] = li[i][9] - li[i-1][9]
#        li[i][10] = li[i][10] - li[i-1][10]

df3 = pd.concat(li) #concat li. df3 is final dataframe
df3 = df3[[0,1,2,3,4,5,6,9,10,11,12,13,14,15,16,17]]
df3.columns = ['date_ymdh', 'ten_cd', 'sku_cd', 'dpt_cd', 'line_cd', 'class_cd', 'sku_name', 'urisu', 'urikin', 'gsagsu1', 'gsaggk1', 'gsagsu2', 'gsaggk2', 'gsagsu3', 'gsaggk3', 'garari']

dbname = os.getenv('REDSHIFT_DB')
host = os.getenv('REDSHIFT_HOST')
port = os.getenv('REDSHIFT_PORT')
user = os.getenv('REDSHIFT_USER')
password = os.getenv('REDSHIFT_PASS')

pr.connect_to_redshift(dbname = dbname,
                        host = host,
                        port = port,
                        user = user,
                        password = password)

pr.connect_to_s3(aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY'),
                bucket = 'nichiji-tmp'
                )

pr.pandas_to_redshift(data_frame = df3,
                        redshift_table_name = 'jisseki_nichiji',
                        append = True)
