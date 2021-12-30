import boto3
import datetime
import time



BUCKET_NAME = 'aws-glue-toretto01'

year = datetime.date.today().year
month = datetime.date.today().month
day = datetime.date.today().day
date = str(year)+'-'+str(month)+'-'+str(day)


## event data
## s3 업로드

s3 = boto3.client('s3')

try:
    reponse = s3.upload_file('./eventdata/part-00000', 
                             BUCKET_NAME, 
                             'input/event/'+str(year)+'/'+str(month)+'/'+str(day)+'/part-00000')
    print('Successfully uploaded EventData')
except:
    print('error in uploading EventData')


## glue job 생성

client = boto3.client('glue')

role = 'service-role/AWSGlueServiceRole-toretto.kim_test'

try:
    response = client.create_job(
        Name = 'toretto-event-{}'.format(date),
        Role = role,
        Command = {
            'Name' : 'glueetl',
            'ScriptLocation' : 's3://{BUCKET_NAME}/scripts/glue-event-job.py'.format(BUCKET_NAME = BUCKET_NAME),
            'PythonVersion' : '3'
        },
        DefaultArguments = {
            '--TempDir' : 's3://{BUCKET_NAME}/temporary'.format(BUCKET_NAME = BUCKET_NAME),
            '--job-bookmark-option' : 'job-bookmark-disable',
            '--enable-spark-ui' : 'true',
            '--spark-event-logs-path' : 's3://{BUCKET_NAME}/sparkHistoryLogs'.format(BUCKET_NAME = BUCKET_NAME)
        },
        MaxRetries = 0,
        GlueVersion = '3.0',
        NumberOfWorkers = 4,
        WorkerType = 'Standard'
    )
    print('Successfully created event-{} job'.format(date))
    
except:
    print('error in creating event-{} job'.format(date))


## glue job 실행

try:
    response = client.start_job_run(
        JobName = 'toretto-event-{}'.format(date)
    )
    print('Successfully started event-{} job'.format(date))
    jobrunid = response['JobRunId']
except:
    print('error in starting event-{} job'.format(date))


## glue job status 체크 (완료 대기)

jobrunstate = 'RUNNING'
while jobrunstate != 'SUCCEEDED':
    response1 = client.get_job_run(JobName = 'toretto-event-{}'.format(date), 
                                   RunId = jobrunid)
    jobrunstate = response1['JobRun']['JobRunState']
    print('event etl job is ' + jobrunstate)
    time.sleep(12)


## glue crawler 생성
try:
    response = client.create_crawler(
        Name='glue-toretto-event-{}'.format(date),
        Role=role,
        DatabaseName='toretto',
        Targets={
            'S3Targets': [
                    {
                        'Path': 's3://{BUCKET_NAME}/output/event/{year}/{month}/{day}/'.format(BUCKET_NAME = BUCKET_NAME,
                                           year = year,
                                           month = month,
                                           day = day),
                    }
                        ]
                },
        TablePrefix='event-{}'.format(date[:-2])
    )
    print("Successfully created event crawler")
except:
    print("error in creating event crawler")

## glue crawler 실행 및 status 체크 (완료 대기)

try:
    response = client.start_crawler(
        Name='glue-toretto-event-{}'.format(date)
    )
    print("Successfully started event crawler")
    
    crawlerstate = 'RUNNING'
    i = 0

    while (crawlerstate == 'RUNNING') or (crawlerstate == 'STARTING'):
        crawlerstate = client.get_crawler(Name = 'glue-toretto-event-{}'.format(date))['Crawler']['State']
        time.sleep(1)
        i += 1
        if i%10 == 0:
            print('event crawler is ' + crawlerstate)

    print('Event Data Processing Finished')
except:
    print("error in starting event crawler")




### attribution
## s3 업로드

try:
    reponse = s3.upload_file('./attributiondata/part-00000', 
                             BUCKET_NAME, 
                             'input/attribution/'+str(year)+'/'+str(month)+'/'+str(day)+'/part-00000')
    print('Successfully uploaded AttributionData')
except:
    print('error in uploading AttributionData')


## glue job 생성

client = boto3.client('glue')

role = 'service-role/AWSGlueServiceRole-toretto.kim_test'

try:
    response = client.create_job(
        Name = 'toretto-attribution-{}'.format(date),
        Role = role,
        Command = {
            'Name' : 'glueetl',
            'ScriptLocation' : 's3://{BUCKET_NAME}/scripts/glue-attribution-job.py'.format(BUCKET_NAME = BUCKET_NAME),
            'PythonVersion' : '3'
        },
        DefaultArguments = {
            '--TempDir' : 's3://{BUCKET_NAME}/temporary'.format(BUCKET_NAME = BUCKET_NAME),
            '--job-bookmark-option' : 'job-bookmark-disable',
            '--enable-spark-ui' : 'true',
            '--spark-event-logs-path' : 's3://{BUCKET_NAME}/sparkHistoryLogs'.format(BUCKET_NAME = BUCKET_NAME)
        },
        MaxRetries = 0,
        GlueVersion = '3.0',
        NumberOfWorkers = 4,
        WorkerType = 'Standard'
    )
    print('Successfully created attribution-{} job'.format(date))
    
except:
    print('error in creating attribution-{} job'.format(date))


## glue job 실행

try:
    response = client.start_job_run(
        JobName = 'toretto-attribution-{}'.format(date)
    )
    print('Successfully started attribution-{} job'.format(date))
    jobrunid = response['JobRunId']
except:
    print('error in starting attribution-{} job'.format(date))


## glue job status 체크 (완료 대기)

jobrunstate = 'RUNNING'
while jobrunstate != 'SUCCEEDED':
    response1 = client.get_job_run(JobName = 'toretto-attribution-{}'.format(date), 
                                   RunId = jobrunid)
    jobrunstate = response1['JobRun']['JobRunState']
    print('attribution etl job is ' + jobrunstate)
    time.sleep(12)


## glue crawler 생성

try:
    response = client.create_crawler(
        Name='glue-toretto-attribution-{}'.format(date),
        Role=role,
        DatabaseName='toretto',
        Targets={
            'S3Targets': [
                    {
                        'Path': 's3://{BUCKET_NAME}/output/attribution/{year}/{month}/{day}/'.format(BUCKET_NAME = BUCKET_NAME,
                                           year = year,
                                           month = month,
                                           day = day),
                    }
                        ]
                },
        TablePrefix='attribution-{}'.format(date[:-2])
    )
    print("Successfully created attribution crawler")
except:
    print("error in creating attribution crawler")


## glue crawler 실행 및 status 체크 (완료 대기)

try:
    response = client.start_crawler(
        Name='glue-toretto-attribution-{}'.format(date)
    )
    print("Successfully started attribution crawler")
    
    crawlerstate = 'RUNNING'
    i = 0

    while (crawlerstate == 'RUNNING') or (crawlerstate == 'STARTING'):
        crawlerstate = client.get_crawler(Name = 'glue-toretto-attribution-{}'.format(date))['Crawler']['State']
        time.sleep(1)
        i += 1
        if i%10 == 0:
            print('attribution crawler is '+ crawlerstate)

    print('Attribution Data Processing Finished')
except:
    print("error in starting attribution crawler")


## 종료 메세지

print('All Processes Finished')
print('You Can Use Athena To Query Now')
