# coding: utf-8

def lambda_handler(event, context):
    #import {{{
    import boto3
    import pandas as pd
    #}}}
    
    s3 = boto3.client('s3')
    
    #Make usecols {{{
    colss_li = [i for i in range(0,211)]
    del colss_li[11:21]
    colss_li.remove(2)
    colss_li.remove(7)
    #}}}

    day = file_name[37:45] #day string

    bucket_name = 'ld-rawdata'
    file_name = 'TR_JISSEKI/20161115XXXXXX/TR_JISSEKI_20161115232207.csv'#TODO
    df = pd.read_csv('s3n://'+bucket_name+'/'+file_name,
            encoding="cp932", 
            header=None, 
            #nrows=10, 
            usecols=colss_li)
    
    
    li = []
    df = df[df[0].isin([day])]
    starth = 7
    base = df.loc[:, 0:10]
    
    #Make hours list
    for i in range(19):
        base.loc[:, 0] = day + '{0:02d}'.format(starth)
        starth+=1
        li.append(pd.concat([base, df.loc[:, 21+i*10:30+i*10]], axis=1))
    
    #set columns
    for i in range(len(li)):
        li[i].columns = [j for j in range(19)]
    
    #difference
    for i in reversed(range(len(li))):
        if i != 0:
            li[i][9] = li[i][9] - li[i-1][9]
            li[i][10] = li[i][10] - li[i-1][10]
    
    df3 = pd.concat(li) #concat li. df3 is final dataframe
