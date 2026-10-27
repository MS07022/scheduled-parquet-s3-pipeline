import os, time
import boto3

s3 = boto3.client("s3")
ddb = boto3.resource("dynamodb")
table = ddb.Table(os.environ["STATE_TABLE"])
raw_bucket = os.environ["RAW_BUCKET"]
quarantine_bucket = os.environ["QUARANTINE_BUCKET"]
quarantine_prefix = os.environ.get("QUARANTINE_PREFIX", "rejected/")


def handler(event, context):
    files = event.get("files", [])
    for obj in files:
        key = obj["key"]
        dest_key = f"{quarantine_prefix}{key}"
        s3.copy_object(Bucket=quarantine_bucket, Key=dest_key,
                       CopySource={"Bucket":obj.get("bucket", raw_bucket), "Key":key}, MetadataDirective="COPY")
        table.update_item(Key={"object_id":f"{obj.get('bucket', raw_bucket)}#{key}"},
                          UpdateExpression="SET #s=:q, reject_reason=:reason, quarantine_bucket=:b, quarantine_key=:k, updated_at=:n",
                          ExpressionAttributeNames={"#s":"status"},
                          ExpressionAttributeValues={":q":"quarantined", ":reason":obj.get("reason","rejected"),
                                                     ":b":quarantine_bucket, ":k":dest_key, ":n":int(time.time())})
    return {"count":len(files)}
