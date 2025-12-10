import boto3
#import json 
import pytz
from io import StringIO
import pandas as pd
from pytz import timezone
import datetime
from datetime import datetime as dt
import re

#ist = pytz.timezone('Asia/Calcutta')
bookmarkBucket = 'mli-dl-prod-s3-raw'
s3 = boto3.client('s3')

print('Starting Function')
#CurrTime = str(datetime.datetime.now(ist))
current_time=dt.now(timezone("Asia/Kolkata")).strftime('%Y%m%d%H%M%S%f')

def lambda_handler(event, context):
    
    print(event)
    file = event['Records'][0]['s3']['object']['key']
    print(file)
    control = 'control'
    extracted = file.split('/')
    print(extracted[1])
    s3BucketARN = event['Records'][0]['s3']['bucket']['arn']
    
    print(s3BucketARN)
    
    for record in event['Records']:
        
        if file.find(control) == -1:
    
            bookmark_rdh = {
        
                'eventTime' : record['eventTime'],
                'eventName' : record['eventName'],
                'userIdentity': record['userIdentity']['principalId'],
                'sourceIPAddress': record['requestParameters']['sourceIPAddress'],
                'bucketName':record['s3']['bucket']['name'],
                'bucketArn':record['s3']['bucket']['arn'],
                'fileName':record['s3']['object']['key'],
                'fileSize':record['s3']['object']['size'],
                'fileeTag':record['s3']['object']['eTag'],
                'sequencer':record['s3']['object']['sequencer'],
                'sourceSystem': extracted[1],
                "dataLayer" : "RTDataHub",
                "status" : "Unprocessed"
            }
            
            bookmark_curated = {
        
                'eventTime' : record['eventTime'],
                'eventName' : record['eventName'],
                'userIdentity': record['userIdentity']['principalId'],
                'sourceIPAddress': record['requestParameters']['sourceIPAddress'],
                'bucketName':record['s3']['bucket']['name'],
                'bucketArn':record['s3']['bucket']['arn'],
                'fileName':record['s3']['object']['key'],
                'fileSize':record['s3']['object']['size'],
                'fileeTag':record['s3']['object']['eTag'],
                'sequencer':record['s3']['object']['sequencer'],
                'sourceSystem': extracted[1],
                "dataLayer" : "Curated",
                "status" : "Unprocessed"
            }
        
            bookmark_rdh_DF = pd.DataFrame([bookmark_rdh])
            bookmark_curated_DF = pd.DataFrame([bookmark_curated])
            csv_buf_rdh = StringIO()
            bookmark_rdh_DF.to_csv(csv_buf_rdh, header=True, index=False)
            csv_buf_cur = StringIO()
            bookmark_curated_DF.to_csv(csv_buf_cur, header=True, index=False)
            csv_buf_rdh.seek(0)
            csv_buf_cur.seek(0)
            
            if s3BucketARN == 'arn:aws:s3:::mli-dl-prod-s3-curated' :
                print("inside curated if")
                s3.put_object(Bucket=bookmarkBucket, Body=csv_buf_cur.getvalue(), Key='bookmarks/curatedSourceData/unprocessed/'+extracted[1]+'/'+extracted[2]+'/'+extracted[3]+'/'+extracted[4]+'.parquet')

            elif extracted[1] == 'Zeus' or extracted[1] == 'Whatsapp' or extracted[1] == 'Posv' or extracted[1] == 'NeoProposal' or extracted[1] == 'mquote':
                s3.put_object(Bucket=bookmarkBucket, Body=csv_buf_rdh.getvalue(), Key='bookmarks/rdh/unprocessed/'+extracted[1]+'/'+extracted[2]+'/'+extracted[7]+'.csv')
                s3.put_object(Bucket=bookmarkBucket, Body=csv_buf_rdh.getvalue(), Key='bookmarks/rdh/unprocessed/'+extracted[1]+'/'+extracted[2]+'-rider/'+extracted[7]+'.csv')                 
                s3.put_object(Bucket=bookmarkBucket, Body=csv_buf_cur.getvalue(), Key='bookmarks/curated/unprocessed/'+extracted[1]+'/'+extracted[2]+'/'+extracted[7]+'.csv') 
                s3.put_object(Bucket=bookmarkBucket, Body=csv_buf_rdh.getvalue(), Key='bookmarks/rds/unprocessed/'+extracted[1]+'/'+extracted[2]+'/'+extracted[7]+'.csv')
            elif extracted[1].lower() == 'telemer' :
                s3.put_object(Bucket=bookmarkBucket, Body=csv_buf_rdh.getvalue(), Key='bookmarks/rdh/unprocessed/'+extracted[1]+'/'+extracted[2]+'/'+extracted[3]+'/base/'+extracted[8]+'.csv') 
                s3.put_object(Bucket=bookmarkBucket, Body=csv_buf_cur.getvalue(), Key='bookmarks/curated/unprocessed/'+extracted[1]+'/'+extracted[2]+'/'+extracted[3]+'/base/'+extracted[8]+'.csv')
                s3.put_object(Bucket=bookmarkBucket, Body=csv_buf_rdh.getvalue(), Key='bookmarks/rds/unprocessed/'+extracted[1]+'/'+extracted[2]+'/'+extracted[3]+'/base/'+extracted[8]+'.csv') 
                s3.put_object(Bucket=bookmarkBucket, Body=csv_buf_rdh.getvalue(), Key='bookmarks/rdh/unprocessed/'+extracted[1]+'/'+extracted[2]+'/'+extracted[3]+'/mdchck/'+extracted[8]+'.csv') 
                s3.put_object(Bucket=bookmarkBucket, Body=csv_buf_cur.getvalue(), Key='bookmarks/curated/unprocessed/'+extracted[1]+'/'+extracted[2]+'/'+extracted[3]+'/mdchck/'+extracted[8]+'.csv')
                s3.put_object(Bucket=bookmarkBucket, Body=csv_buf_rdh.getvalue(), Key='bookmarks/rds/unprocessed/'+extracted[1]+'/'+extracted[2]+'/'+extracted[3]+'/mdchck/'+extracted[8]+'.csv') 
            elif extracted[0].lower() == 'rds_src_data_1' :
                s3.put_object(Bucket=bookmarkBucket, Body=csv_buf_rdh.getvalue(), Key='bookmarks/rdh/unprocessed/'+extracted[1]+'/'+extracted[2]+'/'+extracted[3]+'/'+str(current_time)+'.csv')
                s3.put_object(Bucket=bookmarkBucket, Body=csv_buf_rdh.getvalue(), Key='bookmarks/rdh/unprocessed/'+extracted[1]+'/'+extracted[2]+'/'+extracted[3]+'_1/'+str(current_time)+'.csv')
                s3.put_object(Bucket=bookmarkBucket, Body=csv_buf_rdh.getvalue(), Key='bookmarks/rdh/unprocessed/'+extracted[1]+'/'+extracted[2]+'/'+extracted[3]+'_2/'+str(current_time)+'.csv')
                s3.put_object(Bucket=bookmarkBucket, Body=csv_buf_rdh.getvalue(), Key='bookmarks/rdh/unprocessed/'+extracted[1]+'/'+extracted[2]+'/'+extracted[3]+'_3/'+str(current_time)+'.csv')
                s3.put_object(Bucket=bookmarkBucket, Body=csv_buf_rdh.getvalue(), Key='bookmarks/rdh/unprocessed/'+extracted[1]+'/'+extracted[2]+'/'+extracted[3]+'_4/'+str(current_time)+'.csv')
                s3.put_object(Bucket=bookmarkBucket, Body=csv_buf_rdh.getvalue(), Key='bookmarks/rdh/unprocessed/'+extracted[1]+'/'+extracted[2]+'/'+extracted[3]+'_5/'+str(current_time)+'.csv')
                s3.put_object(Bucket=bookmarkBucket, Body=csv_buf_rdh.getvalue(), Key='bookmarks/rdh/unprocessed/'+extracted[1]+'/'+extracted[2]+'/'+extracted[3]+'_6/'+str(current_time)+'.csv')
                s3.put_object(Bucket=bookmarkBucket, Body=csv_buf_rdh.getvalue(), Key='bookmarks/rdh/unprocessed/'+extracted[1]+'/'+extracted[2]+'/'+extracted[3]+'_COVERAGEDETAILS/'+str(current_time)+'.csv')
                s3.put_object(Bucket=bookmarkBucket, Body=csv_buf_rdh.getvalue(), Key='bookmarks/rdh/unprocessed/'+extracted[1]+'/'+extracted[2]+'/'+extracted[3]+'_POLICYMESURES/'+str(current_time)+'.csv')
                s3.put_object(Bucket=bookmarkBucket, Body=csv_buf_rdh.getvalue(), Key='bookmarks/rdh/unprocessed/'+extracted[1]+'/'+extracted[2]+'/'+extracted[3]+'_BASEATTRIBUTES/'+str(current_time)+'.csv')
                s3.put_object(Bucket=bookmarkBucket, Body=csv_buf_rdh.getvalue(), Key='bookmarks/rdh/unprocessed/'+extracted[1]+'/'+extracted[2]+'/'+extracted[3]+'_POLICYBASICDETAILS/'+str(current_time)+'.csv')
                s3.put_object(Bucket=bookmarkBucket, Body=csv_buf_rdh.getvalue(), Key='bookmarks/rdh/unprocessed/'+extracted[1]+'/'+extracted[2]+'/'+extracted[3]+'_CLIENTPOLICYRELATIONSHIP/'+str(current_time)+'.csv')
            
            elif extracted[3].startswith('MLI_Schema') and extracted[1] == 'msales':
                table = extracted[3].split('.')
                print("qlik path for msales :",'bookmarks/qlik/curated/unprocessed/'+extracted[1]+'/'+table[0]+'/'+table[1]+'/'+extracted[4]+'.csv')
                s3.put_object(Bucket=bookmarkBucket, Body=csv_buf_cur.getvalue(), Key='bookmarks/qlik/curated/unprocessed/'+extracted[1]+'/'+table[0]+'/'+table[1]+'/'+extracted[4]+'.csv')

            elif extracted[3].startswith('SCOPP') or extracted[3].startswith('SMSUSER'):
                #table = extracted[3].split('.')
                table_nm = re.split('\.|__',extracted[3])
                print("qlik path for SCOPP and SMSUSER :",'bookmarks/qlik/curated/unprocessed/'+extracted[1]+'/'+extracted[2]+'/'+table_nm[1]+'/'+extracted[5]+'.csv')
                s3.put_object(Bucket=bookmarkBucket, Body=csv_buf_cur.getvalue(), Key='bookmarks/qlik/curated/unprocessed/'+extracted[1]+'/'+extracted[2]+'/'+table_nm[1]+'/'+extracted[5]+'.csv')

            else :  
                requirement_arr=("BASEATTRIBUTES","ACCOUNTDETAIL","CLIENTPOLICYRELATIONSHIP","BASEADDDETAIL","COVERAGEDETAILS","POLICYMESURES","BONUSDETAILS","PARTWITHDRWLDETAILS","PAYMENTHISTORYDTLS","LOANDETAIL","CREDITCARDDETAIL","POLICYBASICDETAILS","NOMINEEDETAILS")  
                
                if extracted[4].startswith('KCAPUAT3') or extracted[4].startswith('MNYLMIG'):
                    print("qlik path for ingenium :",extracted[1]+'/'+extracted[2]+'/'+extracted[3]+'/'+extracted[4]+'/'+extracted[6]+'.csv')
                    table_nm = re.split('\.|__',extracted[4])
                   # print("Getting table name :",table_nm,"from "extracted[3])
                    s3.put_object(Bucket=bookmarkBucket, Body=csv_buf_rdh.getvalue(), Key='bookmarks/qlik/rdh/unprocessed/'+extracted[1]+'/'+extracted[2]+'/'+table_nm[1]+'/'+extracted[6]+'.csv') 
                    s3.put_object(Bucket=bookmarkBucket, Body=csv_buf_cur.getvalue(), Key='bookmarks/qlik/curated/unprocessed/'+extracted[1]+'/'+extracted[2]+'/'+table_nm[1]+'/'+extracted[6]+'.csv')
                    s3.put_object(Bucket=bookmarkBucket, Body=csv_buf_rdh.getvalue(), Key='bookmarks/qlik/rds/unprocessed/'+extracted[1]+'/'+extracted[2]+'/'+table_nm[1]+'/'+extracted[6]+'.csv')
                    for name in requirement_arr:
                        s3.put_object(Bucket=bookmarkBucket, Body=csv_buf_rdh.getvalue(), Key='bookmarks/qlik/rdh/unprocessed/'+extracted[1]+'/'+extracted[2]+'/'+table_nm[1]+'_'+str(name)+'/'+extracted[6]+'.csv')
                print("************************************** Getting ms-sql file from Qlik **************************************")
                #ms-sql
                if extracted[4].startswith('dbo'):
                    print("qlik path for dbo :",extracted[1]+'/'+extracted[2]+'/'+extracted[3]+'/'+extracted[4]+'/'+extracted[6]+'.csv')
                    table_nm = re.split('\.|__',extracted[4])
                    s3.put_object(Bucket=bookmarkBucket, Body=csv_buf_cur.getvalue(), Key='bookmarks/qlik/curated/unprocessed/'+extracted[1]+'/'+extracted[2]+'/'+table_nm[1]+'/'+extracted[6]+'.csv')

                if extracted[4].startswith('SMSUSER') or extracted[4].startswith('RENOVAPRODIND'):
                    print("qlik path of other system :",extracted[1]+'/'+extracted[2]+'/'+extracted[3]+'/'+extracted[4]+'/'+extracted[6]+'.csv')
                    table_nm = re.split('\.|__',extracted[4])
                    s3.put_object(Bucket=bookmarkBucket, Body=csv_buf_cur.getvalue(), Key='bookmarks/qlik/curated/unprocessed/'+extracted[1]+'/'+extracted[2]+'/'+table_nm[1]+'/'+extracted[6]+'.csv')
                if extracted[1].startswith('daksh'):
                    print("daksh table")
                    s3.put_object(Bucket=bookmarkBucket, Body=csv_buf_cur.getvalue(), Key='bookmarks/qlik/curated/unprocessed/'+extracted[1]+'/'+extracted[5]+'.csv')
                    
                for name in requirement_arr:
                     #print(name)
                    s3.put_object(Bucket=bookmarkBucket, Body=csv_buf_rdh.getvalue(), Key='bookmarks/rdh/unprocessed/'+extracted[1]+'/'+extracted[2]+'/'+extracted[3]+'_'+str(name)+'/'+extracted[8]+'.csv')
                s3.put_object(Bucket=bookmarkBucket, Body=csv_buf_rdh.getvalue(), Key='bookmarks/rdh/unprocessed/'+extracted[1]+'/'+extracted[2]+'/'+extracted[3]+'/'+extracted[8]+'.csv') 
                s3.put_object(Bucket=bookmarkBucket, Body=csv_buf_cur.getvalue(), Key='bookmarks/curated/unprocessed/'+extracted[1]+'/'+extracted[2]+'/'+extracted[3]+'/'+extracted[8]+'.csv')
                s3.put_object(Bucket=bookmarkBucket, Body=csv_buf_rdh.getvalue(), Key='bookmarks/rds/unprocessed/'+extracted[1]+'/'+extracted[2]+'/'+extracted[3]+'/'+extracted[8]+'.csv')
                


             
    
    return {
        'statusCode': 200,
        'body': 'successful'
    }
