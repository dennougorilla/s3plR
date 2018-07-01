# coding: utf-8

def lambda_handler(event, context):
    import boto3
    
    s3 = boto3.client('s3')
    
    bucket_name = 'ld-rawdata'
    file_name = 'TR_JISSEKI/20160501XXXXXX/TR_JISSEKI_20160501002205.csv'
    response = s3.get_object(Bucket=bucket_name, Key=file_name)
    body = response['Body'].read()

    bodystr = body.decode('utf-8')
    lines = bodystr.split('\r\n')
    
    print(lines)

import boto3
import pandas as pd

s3 = boto3.client('s3')

colss_li = [i for i in range(0,211)]
del colss_li[11:21]
colss_li.remove(2)
colss_li.remove(7)
bucket_name = 'ld-rawdata'
file_name = 'TR_JISSEKI/20161115XXXXXX/TR_JISSEKI_20161115232207.csv'
day = file_name[37:45]
df = pd.read_csv('s3n://'+bucket_name+'/'+file_name,encoding="cp932", 
        header=None, 
        #nrows=10, 
        usecols=colss_li)


li = []
df = df[df[0].isin([day])]
starth = 7
base = df.loc[:, 0:10]

for i in range(19):
    base.loc[:, 0] = day + '{0:02d}'.format(starth)
    starth+=1
    li.append(pd.concat([base, df.loc[:, 21+i*10:30+i*10]], axis=1))

for i in range(len(li)):
    li[i].columns = [j for j in range(19)]

for i in reversed(range(len(li))):
    if i != 0:
        li[i][9] = li[i][9] - li[i-1][9]
        li[i][10] = li[i][10] - li[i-1][10]

df3 = pd.concat(li)

df3.to_csv('test_output.csv', index = False)
