# Import necessary libraries and modules
import boto3
import uuid
import time
import datetime
import asyncio
import logging
from botocore.client import ClientError

# Initialize Logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Declare loop at a global scope
loop = None

# Function to set global environment variables
def set_global_vars():
    global_vars = {"status": False}
    try:
        # Set various global variables
        global_vars["Owner"] = "Mystique"
        global_vars["Environment"] = "Prod"
        global_vars["aws_region"] = "us-east-1"
        global_vars["tag_name"] = "cloudwatch_logs_s3_bucket"
        global_vars["retention_days"] = 1
        global_vars["cw_logs_to_export"] = get_all_ecs_log_groups()
        global_vars["log_dest_bkt"] = "cw-logroup-to-s3"
        global_vars["time_out"] = 30000
        global_vars["tsk_back_off"] = 2
        global_vars["status"] = True
    except Exception as e:
        logger.error("Unable to set Global Environment variables. Exiting")
        global_vars["error_message"] = str(e)
    return global_vars

# Function to fetch ECS log groups from CloudWatch
def get_all_ecs_log_groups():
    log_groups = []
    try:
        client = boto3.client('logs')
        # Describe log groups with a prefix of '/aws/ecs/'
        response = client.describe_log_groups(logGroupNamePrefix='/aws/ecs/')
        log_groups = [group['logGroupName'] for group in response.get('logGroups', [])]
    except Exception as e:
        logger.error(f"Unable to fetch CloudWatch log groups: {str(e)}")
    return log_groups

# Async function to export CloudWatch logs to S3 for a given log group
async def export_cw_logs_to_s3(global_vars, log_group_name, retention_days, bucket_name, obj_prefix=None):
    resp_data = {'status': False, 'task_info': {}, 'error_message': ''}
    if not retention_days:
        retention_days = 90
    if not obj_prefix:
        obj_prefix = log_group_name.split('/')[-1]
    now_time = datetime.datetime.now()
    n1_day = now_time - datetime.timedelta(days=int(retention_days) + 1)
    n_day = now_time - datetime.timedelta(days=int(retention_days))
    f_time = int(n1_day.timestamp() * 1000)
    t_time = int(n_day.timestamp() * 1000)
    if '/' in log_group_name:
        d_prefix = str(log_group_name.replace("/", "-")[1:]) + "/" + str(gen_ymd(n1_day, '/'))
    else:
        d_prefix = str(log_group_name.replace("/", "-")) + "/" + str(gen_ymd(n1_day, '/'))

    resp = does_bucket_exists(bucket_name)
    if not resp.get('status'):
        resp_data['error_message'] = resp.get('error_message')
        logger.error(f"Error exporting logs for log group '{log_group_name}': {resp.get('error_message')}")
        return resp_data

    try:
        client = boto3.client('logs')
        # Create an export task
        r = client.create_export_task(
            taskName=gen_uuid(),
            logGroupName=log_group_name,
            fromTime=f_time,
            to=t_time,
            destination=bucket_name,
            destinationPrefix=d_prefix
        )

        # Get the status of each of those asynchronous export tasks
        r = get_tsk_status(r.get('taskId'), global_vars.get('time_out'), global_vars.get('tsk_back_off'))
        if r.get('status'):
            resp_data['task_info'] = r.get('tsk_info')
            resp_data['status'] = True
        else:
            resp_data['error_message'] = r.get('error_message')
    except Exception as e:
        resp_data['error_message'] = str(e)
        logger.error(f"Error exporting logs for log group '{log_group_name}': {resp_data['error_message']}")
    return resp_data

# Async function to export logs for all log groups
async def export_all_logs(global_vars, log_groups):
    export_tasks = [export_cw_logs_to_s3(global_vars, log_group.get('logGroupName'), global_vars.get('retention_days'), global_vars.get('log_dest_bkt')) for log_group in log_groups]
    return await asyncio.gather(*export_tasks)

# Function to get the status of an export task
def get_tsk_status(tsk_id, time_out, tsk_back_off):
    resp_data = {'status': False, 'tsk_info': {}, 'error_message': ''}
    client = boto3.client('logs')
    if not time_out:
        time_out = 30000
    t = tsk_back_off
    try:
        while True:
            time.sleep(t)
            resp = client.describe_export_tasks(taskId=tsk_id)
            tsk_info = resp['exportTasks'][0]
            if t > int(time_out):
                resp_data['error_message'] = f"Task:{tsk_id} is still running. Status:{tsk_info['status']['code']}"
                resp_data['tsk_info'] = tsk_info
                break
            if tsk_info['status']['code'] != "COMPLETED":
                t *= 2
            else:
                resp_data['tsk_info'] = tsk_info
                resp_data['status'] = True
                break
    except Exception as e:
        resp_data['error_message'] = f"Unable to verify status of task:{tsk_id}. ERROR:{str(e)}"
        logger.error(resp_data['error_message'])
    resp_data['tsk_info']['time_taken'] = t
    logger.info(f"It took {t} seconds to export Log Group: '{resp_data.get('tsk_info').get('logGroupName')}'")
    return resp_data

# Function to generate a UUID
def gen_uuid():
    return str(uuid.uuid4())

# # Function to generate year-month-day from epoch time
# def gen_ymd_from_epoch(t):
#     t = t / 1000
#     ymd = (str(datetime.datetime.utcfromtimestamp(t).year) +
#            "-" +
#            str(datetime.datetime.utcfromtimestamp(t).month) +
#            "-" +
#            str(datetime.datetime.utcfromtimestamp(t).day)
#            )
#     return ymd

# Function to generate year-month-day from a datetime object
def gen_ymd(t, d):
    ymd = (str(t.year) + d + str(t.month) + d + str(t.day))
    return ymd

# Function to check if an S3 bucket exists
def does_bucket_exists(bucket_name):
    bucket_exists_status = {'status': False, 'error_message': ''}
    try:
        s3 = boto3.resource('s3')
        s3.meta.client.head_bucket(Bucket=bucket_name)
        bucket_exists_status['status'] = True
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            bucket_exists_status['status'] = False
            bucket_exists_status['error_message'] = str(e)
        else:
            bucket_exists_status['status'] = False
            bucket_exists_status['error_message'] = str(e)
    return bucket_exists_status

# Function to get CloudWatch log groups
def get_cloudwatch_log_groups(global_vars):
    resp_data = {'status': False, 'log_groups': [], 'error_message': ''}
    client = boto3.client('logs')
    try:
        resp = client.describe_log_groups(limit=50)
        resp_data['log_groups'].extend(resp.get('logGroups'))
        if resp.get('nextToken'):
            while True:
                resp = client.describe_log_groups(nextToken=resp.get('nextToken'), limit=50)
                resp_data['log_groups'].extend(resp.get('logGroups'))
                if not resp.get('nextToken'):
                    break
        resp_data['status'] = True
    except Exception as e:
        resp_data['error_message'] = str(e)
        logger.error(f"Error getting CloudWatch log groups: {resp_data['error_message']}")
    return resp_data

# Function to filter logs based on the predefined criteria
def filter_logs_to_export(global_vars, lgs):
    resp_data = {'status': False, 'log_groups': [], 'error_message': ''}
    for lg in lgs.get('log_groups'):
        if lg.get('logGroupName') in global_vars.get('cw_logs_to_export'):
            resp_data['log_groups'].append(lg)
            resp_data['status'] = True
    return resp_data

# AWS Lambda handler function
def lambda_handler(event, context):
    global loop
    global_vars = set_global_vars()
    resp_data = {"status": False, "error_message": ''}

    if not global_vars.get('status'):
        logger.error(f'Unable to set global variables: {global_vars.get("error_message")}')
        resp_data['error_message'] = global_vars.get('error_message')
        return resp_data

    lgs = get_cloudwatch_log_groups(global_vars)
    if not lgs.get('status'):
        logger.error("Unable to get list of CloudWatch Logs.")
        resp_data['error_message'] = lgs.get('error_message')
        return resp_data

    f_lgs = filter_logs_to_export(global_vars, lgs)
    if not (f_lgs.get('status') or f_lgs.get('log_groups')):
        err = f"There are no log groups matching the filter or unable to get a filtered list of CloudWatch Logs."
        logger.error(err)
        resp_data['error_message'] = f"{err} ERROR:{f_lgs.get('error_message')}"
        resp_data['lgs'] = {'all_logs': lgs, 'cw_logs_to_export': global_vars.get('cw_logs_to_export'), 'filtered_logs': f_lgs}
        return resp_data

    loop = asyncio.new_event_loop() #The asyncio library is used for asynchronous programming.
    asyncio.set_event_loop(loop)

    try:
        resp_data['export_tasks'] = loop.run_until_complete(export_all_logs(global_vars, f_lgs.get('log_groups')))
        resp_data['status'] = True
    finally:
        loop.close()

    return resp_data

# Entry point when running the script as the main module
if __name__ == '__main__':
    lambda_handler(None, None)
