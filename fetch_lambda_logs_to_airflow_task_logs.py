from __future__ import annotations
import json
from datetime import datetime, timezone
import time
import boto3
import os

from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
from airflow.operators.python import PythonOperator


def invoke_lambda_and_fetch_logs(function_name: str, payload: dict, ti=None):
    """
    Invokes a Lambda function and fetches its full log stream to the Airflow logs,
    retrying a few times to ensure all logs are ingested by CloudWatch.
    """
    log = ti.log
    aws_hook = AwsBaseHook(aws_conn_id="aws_default", client_type="lambda")
    lambda_client = aws_hook.get_conn()
    logs_client = boto3.client("logs", region_name=lambda_client.meta.region_name)
    log_group_name = f"/aws/lambda/{function_name}"
    
    log.info(f"Invoking Lambda function: {function_name}")
    
    response = lambda_client.invoke(
        FunctionName=function_name,
        InvocationType='RequestResponse',
        Payload=json.dumps(payload),
    )
    
    response_payload = json.loads(response['Payload'].read().decode('utf-8'))
    request_id = response['ResponseMetadata']['RequestId']

    log.info(f"Lambda executed with RequestId: {request_id}")
    log.info(f"Response Payload: {response_payload}")

    log_stream_name = None
    body_dict = {}
    
    # Checking Lambda Response payload for log_stream_name gracefully
    try:
        if 'body' in response_payload:
            body_dict = json.loads(response_payload['body'])
            log_stream_name = body_dict.get('log_stream_name')
        else:
            log.warning("Lambda response payload did not contain a 'body' key.")
    except (json.JSONDecodeError, KeyError) as e:
        log.warning(f"Could not parse Lambda response body, cannot fetch logs. Error: {e}")

    if response.get("FunctionError"):
        raise Exception(f"Lambda function execution failed (RequestId: {request_id}): {body_dict}")

    if not log_stream_name:
        log.warning("Lambda did not return a log_stream_name. Please go to CloudWatch to look for logs.")
        return body_dict
    
    log.info(f"Waiting for log stream '{log_stream_name}' to become available...")
    for attempt in range(5): # Retry up to 5 times
        try:
            streams = logs_client.describe_log_streams(
                logGroupName=log_group_name,
                logStreamNamePrefix=log_stream_name
            )
            if streams['logStreams']:break
        except logs_client.exceptions.ResourceNotFoundException:
            log.warning(f"Attempt {attempt + 1}: Log stream not yet found. Retrying in 5 seconds...")
            time.sleep(5)
        except Exception as e: 
            log.error(f"An unexpected error occurred while describing log streams: {e}") 
            raise e


    log.info(f"Attempting to fetch logs from stream")

    # Fetching logs with Pagination, Retries, and Log Filtering
    # This Retry strategy waits for both START and END signals for the corresponding requestID

    try:
        next_token = None
        found_start_marker = False
        found_end_marker = False
        
        retries = 7
        # Try up to  times to get the complete log (START to END)
        for attempt in range(retries):

            log.info(f"--- Log Fetch Attempt {attempt + 1}/{retries} for RequestId: {request_id} ---")
            
            while True:
                kwargs = {'logGroupName': log_group_name, 'logStreamName': log_stream_name, 'startFromHead': True}
                if next_token:
                    kwargs['nextToken'] = next_token

                log_events_response = logs_client.get_log_events(**kwargs)

                for event in log_events_response['events']:
                    message = event['message']

                    if not found_start_marker and "START RequestId: " + request_id in message:
                        found_start_marker = True
                    
                    if found_start_marker:
                        log.info(message.strip())

                    if "END RequestId: " + request_id in message:
                        found_end_marker = True
                        break
                
                if found_end_marker:
                    break
                
                new_token = log_events_response.get('nextForwardToken')
                if new_token and new_token != next_token:
                    next_token = new_token
                else:
                    break # Reached end of available logs for this attempt
            
            if found_end_marker:
                log.info("--- End of Log ---")
                return body_dict
            
            if not found_start_marker:
                log.warning(f"Attempt {attempt + 1}: Did not find START marker. CloudWatch logs may still be ingesting. Retrying in 5 seconds...")
            # else: Commenting this out as it adds logs in between lambda CW logs
            #     log.warning(f"Attempt {attempt + 1}: Found START but not END marker. Retrying in 5 seconds...")
            
            time.sleep(5)

        log.error(f"Failed to fetch complete logs for RequestId: {request_id} after all retries.")

    except Exception as e:
        log.error(f"An unexpected error occurred while fetching CloudWatch logs. Reason: {e}")

    return body_dict
