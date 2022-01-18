import json
import pandas as pd
import os
import logging
import re

# import boto3
import datetime
import pymongo
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
import psycopg2

database = 'mongoDB'

# Define configured variables to connect to Mongo and AWS S3
mongo_config = Variable.get("mongo_config", deserialize_json=True)
s3_config = Variable.get("aws_s3_config", deserialize_json=True)

if database == 'mongoDB':
    client = pymongo.MongoClient(
        host=mongo_config['host'],
        port=mongo_config['port'],
        username=mongo_config['username'],
        password=mongo_config['password'])

    # select database
    db = client.testairflow

else:
    # Amazon Redshift database connection details
    dbname = ''
    host = ''
    port = ''
    user = ''
    password = ''
    awsIAMrole = ''


def getDBdate(ti):
    """
    Args:
        ti: task instance argument used by Airflow to push and pull xcom data

    push recent date searching with mongo and store in xcom of airflow 
    """

    try:
        if database == 'mongoDB':
            # >>> use the MONGO database
            # check the field names 'dateFor' has to be exist
            aggFilter = {'dateFor': {'$exists': True}}
            dateDoc = list(db.countyDiff.aggregate([{'$match': aggFilter},
                                                    {'$project': {
                                                        'date': {
                                                            '$dateFromString': {
                                                                'dateString': '$dateFor',
                                                                'format': '%Y-%m-%d'}
                                                        }
                                                    }},
                                                    {'$sort': {'date': -1}},
                                                    {'$limit': 1}
                                                    ]))

            # if nothing
            try:
                fetchedDate = dateDoc[0]['date'].strftime('%Y-%m-%d')
            except:
                fetchedDate = '2022-01-01'

        else:
            # >>> use the AMAZON REDSHIFT database
            # set up the connection to the Redshift database
            conn = psycopg2.connect(
                f'dbname={dbname} host={host} port={port} user={user} password={password}')
            cursor = conn.cursor()
            sql = """SELECT dateFor FROM counties ORDER BY dateFor DESC LIMIT 1;"""
            try:
                cursor.execute(sql)
                fetchedDate = cursor.fetchall()[0][0].strftime('%Y-%m-%d')
                logging.info(fetchedDate)
                # close the connection and cursor
                cursor.close()
                conn.close()
            except:
                fetchedDate = '2020-01-01'
                cursor.close()
                conn.close()

    except:
        fetchedDate = None

    logging.info(fetchedDate)
    # push the task instance (key, value format) to an xcom
    ti.xcom_push(key='fetchedDate', value=fetchedDate)


def getLastDate(ti):
    """
    Pull the date from xcom and return one task id, or multiple task IDs if they are inside a list, to be executed
    Args:
        ti: task instance argument used by Airflow to push and pull xcom data

    if has 'fetchedDate' in xcom then return job(parseJsonFile) else return job(endRun)
    """

    fetchedDate = ti.xcom_pull(key='fetchedDate', task_ids=[
                               'getLastProcessedDate'])

    if fetchedDate[0] is not None:
        return 'parseJsonFile'
    return 'endRun'


def readJsonData(ti):
    """
    Read and parse a Json, save the parsed data to a CSV
    Args:
        ti: task instance argument used by Airflow to push and pull xcom data

    Returns: a task id to be executed
    """

    fetchedDate = ti.xcom_pull(key='fetchedDate', task_ids=[
                               'getLastProcessedDate'])

    lastDBDate = datetime.datetime.strptime(fetchedDate[0], '%Y-%m-%d')

    hook = S3Hook()
    key = s3_config['key']
    bucket = s3_config['bucket']
    path = '/opt/airflow/sparkFiles'
    filename = hook.download_file(
        key=key,
        bucket_name=bucket,
        local_path=path
    )
    # open the json data with the correct encoding. 'latin-1' encoding was used as the json contains some chars that
    # are not encoded in utf-8 (which is usually used)
    with open(filename, encoding='latin-1') as data:
        # due to Decode error with escape word so replace except needed word
        rep_data = re.sub('[^":{}\-\[\].,"A-Za-z0-9]', '', data.read())
        jsonData = json.loads(rep_data)

        if 'historicalData' in jsonData.keys():
            dfData = []
            for key in jsonData['historicalData'].keys():
                if lastDBDate <= datetime.datetime.strptime(key, '%Y-%m-%d'):
                    if type(jsonData["historicalData"][key]['countyInfectionsNumbers']) == dict:
                        parsedLine = {}
                        parsedLine['dateFor'] = key
                        parsedLine.update(
                            jsonData["historicalData"][key]['countyInfectionsNumbers'])
                        dfData.append(parsedLine)

            if len(dfData) > 0:
                df = pd.DataFrame(dfData)
                df = df.fillna(0)
                df.to_csv('/opt/airflow/sparkFiles/parsedData.csv',
                          encoding='utf8',
                          index=False,
                          header=True)

                os.remove(filename)

                return 'processParsedData'
            return 'endRun'
        return 'endRun'


def uploadToDB(ti):
    """
    Upload the results data to the database
    """
    results = '/opt/airflow/sparkFiles/results.csv'

    fetchedDate = ti.xcom_pull(key='fetchedDate', task_ids=[
                               'getLastProcessedDate'])
    lastDBDate = fetchedDate[0]

    pandasDf = pd.read_csv(results)
    # combine data
    newDf = pd.concat([pandasDf.loc[pandasDf.dateFor != lastDBDate]])

    if database == 'mongoDB':
        resultsList = newDf.to_dict(orient='records')
        for item in resultsList:
            insertToDB = db.countyDiff.replace_one({'dateFor': item['dateFor']},
                                                   item,
                                                   upsert=True)

    else:
        # >>> using the AMAZON REDSHIFT database
        newDf.to_csv(results,
                     sep=',',
                     header=True,
                     index=False)
        hook = S3Hook()
        key = s3_config['key']
        bucket = s3_config['bucket']
        loadToS3 = hook.load_file(
            filename=results,
            key=key,
            bucket_name=bucket,
            replace=True
        )

        conn = psycopg2.connect(
            f'dbname={dbname} host={host} port={port} user={user} password={password}')
        # start the database cursor
        cursor = conn.cursor()
        sql = f"""COPY counties FROM '' 
                  iam_role '{awsIAMrole}' 
                  DELIMITER AS ',' 
                  DATEFORMAT 'YYYY-MM-DD' 
                  IGNOREHEADER 1 ;"""

        cursor.execute(sql)
        conn.commit()
        cursor.close()
        conn.close()

    os.remove(results)


# set up DAG arguments
defaultArgs = {
    'owner': 'plerin152',
    'start_date': datetime.datetime(2021, 1, 1),
    'retries': 3,
    'retry_delay': datetime.timedelta(seconds=30)
}

with DAG('analyze_json_data',
         schedule_interval='@daily',
         default_args=defaultArgs,
         catchup=True) as dag:

    getLastProcessedDate = PythonOperator(
        task_id='getLastProcessedDate',
        python_callable=getDBdate
    )

    getDate = BranchPythonOperator(
        task_id='getDate',
        python_callable=getLastDate,
        do_xcom_push=False
    )

    parseJsonFile = BranchPythonOperator(
        task_id='parseJsonFile',
        python_callable=readJsonData,
        do_xcom_push=False
    )

    processParsedData = BashOperator(
        task_id='processParsedData',
        bash_command='python /opt/airflow/sparkFiles/sparkProcess.py'
    )

    saveToDB = PythonOperator(
        task_id='saveToDB',
        python_callable=uploadToDB
    )

    endRun = DummyOperator(
        task_id='endRun',
        trigger_rule='none_failed_or_skipped'
    )

    getLastProcessedDate >> getDate
    getDate >> [parseJsonFile, endRun]
    parseJsonFile >> [processParsedData, endRun]
    processParsedData >> saveToDB >> endRun
