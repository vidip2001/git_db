import boto3
import pandas as pd
from io import StringIO

def getexecid(client):
    import time
    queryStart = client.start_query_execution(
        QueryString='SELECT * FROM table_new',
        QueryExecutionContext={
            'Database': 'db_new'
        },
        ResultConfiguration={'OutputLocation': 's3://pipeline-qr/qr'}
    )
    while True:
        data = client.get_query_execution(QueryExecutionId=queryStart['QueryExecutionId'])
        print(data["QueryExecution"]["Status"]["State"])
        time.sleep(5)
        if(data["QueryExecution"]["Status"]["State"]=="SUCCEEDED"):
            return queryStart['QueryExecutionId']


def main(awskey,awssecret):
    queryoutloc = 'pipeline-qr/qr'

    client = boto3.client('athena', region_name='ap-south-1', aws_access_key_id=awskey,
                          aws_secret_access_key=awssecret)
    execid = getexecid(client)
    response = client.get_query_results(
            QueryExecutionId=execid
    )
    results = response['ResultSet']['Rows']

    s3 = boto3.client('s3',region_name='ap-south-1',aws_access_key_id=awskey,
                          aws_secret_access_key=awssecret)
    bucket_name = 'pipeline-qr'
    file_path = f'qr/{execid}.csv'
    response = s3.get_object(Bucket=bucket_name, Key=file_path)
    data = response['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(data), index_col=0)
    print(len(df))

