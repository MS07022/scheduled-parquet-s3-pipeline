import json, os, time
import boto3

sns = boto3.client("sns")
locks = boto3.resource("dynamodb").Table(os.environ["LOCK_TABLE"])


def handler(event, context):
    summary = event["summary"]
    status = event.get("status", "SUCCEEDED")
    try:
        sns.publish(TopicArn=os.environ["SNS_TOPIC_ARN"], Subject=f"S3 Parquet pipeline {status.lower()}",
                    Message=json.dumps({"status":status, **summary, "error":event.get("error")}, indent=2))
    finally:
        locks.delete_item(Key={"lock_name":"pipeline"}, ConditionExpression="run_id=:r",
                          ExpressionAttributeValues={":r":summary["run_id"]})
    return {"published":True, "completed_at":int(time.time())}
