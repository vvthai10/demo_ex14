import os
import datetime
import subprocess
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from luigi.contrib.hdfs import HdfsTarget


CURRENT_DAY_PROCESSED = "2024_11_12"
CURRENT_MONTH_PROCESSED = "2024_11"

def checkFileInput(input_path):
    return HdfsTarget(input_path).exists()

def scheduled_job_chat_hourly_aggregate():
    global CURRENT_DAY_PROCESSED
    dt = datetime.datetime.now()
    print(f"{dt} | worker started, process {CURRENT_DAY_PROCESSED}")
    date_object = datetime.datetime.strptime(CURRENT_DAY_PROCESSED, '%Y_%m_%d').date()
    date_object = date_object + datetime.timedelta(days=1)
    month_object = date_object.strftime("%Y_%m")
    date_object = date_object.strftime("%Y_%m_%d")

    input_path = f"hdfs://localhost:9000/root/chat_logs/{date_object}"
    output_path = f"hdfs://localhost:9000/root/chat_hourly_agg/{month_object}"
    # input_path="/root/chat_logs/2024_test/part-00001-f4318cad-33c0-4232-b88d-044efb2b760f-c000.snappy.parquet"

    # Check input_path exists:
    if not checkFileInput(input_path):
        print("Have not input_path ", input_path)
        return
    
    os.environ['SPARK_HOME'] = '/opt/spark-3.5.3'
    os.environ['PYTHONPATH'] = os.path.join(os.environ['SPARK_HOME'], 'python') + ':' + os.path.join(os.environ['SPARK_HOME'], 'python', 'lib', 'py4j-0.10.9.7-src.zip') + ':.'

    command = [
        "luigi", 
        "--module", "process", 
        "ChatHourlyAggregate",
        "--input-path", input_path,
        "--output-path", output_path,
    ]

    try:
        subprocess.run(command, check=True, env=os.environ)
        CURRENT_DAY_PROCESSED = date_object
        print("Spark job executed success")
    except subprocess.CalledProcessError as e:
        print(f"Error executing Spark job: {e}")

def scheduled_job_chat_monthly_aggregate():
    global CURRENT_MONTH_PROCESSED
    dt = datetime.datetime.now()
    print(f"{dt} | worker started, process {CURRENT_MONTH_PROCESSED}")
    date_object = datetime.datetime.strptime(CURRENT_MONTH_PROCESSED, '%Y_%m').date()
    date_object = date_object + datetime.timedelta(days=30)
    month_object = date_object.strftime("%Y_%m")

    input_path = f"hdfs://localhost:9000/root/chat_hourly_agg/{month_object}"
    # input_path="/root/chat_hourly_agg/2024_11_13/part-00001-5b931710-ec7f-4c33-aa35-c842edc4bb76-c000.snappy.parquet"

    # Check input_path exists:
    if not checkFileInput(input_path):
        print("Have not input_path ", input_path)
        return
    
    os.environ['SPARK_HOME'] = '/opt/spark-3.5.3'
    os.environ['PYTHONPATH'] = os.path.join(os.environ['SPARK_HOME'], 'python') + ':' + os.path.join(os.environ['SPARK_HOME'], 'python', 'lib', 'py4j-0.10.9.7-src.zip') + ':.'

    command = [
        "luigi", 
        "--module", "process", 
        "ChatMonthlyAggregate",
        "--input-path", input_path,
    ]

    try:
        subprocess.run(command, check=True, env=os.environ)
        CURRENT_DAY_PROCESSED = date_object
        print("Spark job executed success")
    except subprocess.CalledProcessError as e:
        print(f"Error executing Spark job: {e}")

def job_listener(event):
    if event.exception:
        print(f"Job {event.job_id} failed")
    else:
        print(f"Job {event.job_id} completed successfully")

if __name__ == '__main__':
    sched = BlockingScheduler()
    # sched.add_job(scheduled_job_chat_hourly_aggregate, 'interval', id='scheduled_job_chat_hourly_aggregate', seconds=10)
    sched.add_job(scheduled_job_chat_monthly_aggregate, 'interval', id='scheduled_job_chat_monthly_aggregate', seconds=10)
    sched.add_listener(job_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
    print("Scheduler started...")
    sched.start()
