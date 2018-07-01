# coding: utf-8
#dennougorilla

#def lambda_handler(event, context):
#import {{{
import boto3
import pandas as pd
from tqdm import tqdm
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
starth = 7
base = df.loc[:, 0:10]

#Make hours list
for i in tqdm(range(19)):
    base.loc[:, 0] = day + '{0:02d}'.format(starth)
    starth+=1
    li.append(pd.concat([base, df.loc[:, 21+i*10:30+i*10]], axis=1))

#set columns
for i in tqdm(range(len(li))):
    li[i].columns = [j for j in range(19)]

#difference
for i in tqdm(reversed(range(len(li)))):
    if i != 0:
        li[i][9] = li[i][9] - li[i-1][9]
        li[i][10] = li[i][10] - li[i-1][10]

df3 = pd.concat(li) #concat li. df3 is final dataframe
df3 = df3[[0,1,2,3,4,5,6,9,10,11,12,13,14,15,16,17]]
df3.columns = ['date_ymdh', 'ten_cd', 'sku_cd', 'dpt_cd', 'line_cd', 'class_cd', 'sku_name', 'urisu', 'urikin', 'gsagsu1', 'gsaggk1', 'gsagsu2', 'gsaggk2', 'gsagsu3', 'gsaggk3', 'garari']
