import os, time, uuid
import boto3
from botocore.exceptions import ClientError

s3 = boto3.client("s3")
ddb = boto3.resource("dynamodb")
state = ddb.Table(os.environ["STATE_TABLE"])
locks = ddb.Table(os.environ["LOCK_TABLE"])
bucket = os.environ["RAW_BUCKET"]
prefix = os.environ.get("RAW_PREFIX", "")
limit = min(int(os.environ.get("BATCH_SIZE", "300")), 300)
claim_seconds = int(os.environ.get("CLAIM_TIMEOUT_MINUTES", "180")) * 60


def claim(obj, run_id):
    now = int(time.time())
    oid = f"{obj['bucket']}#{obj['key']}"
    try:
        state.update_item(
            Key={"object_id": oid},
            UpdateExpression="SET #s=:p, run_id=:r, etag=:e, claimed_at=:n, updated_at=:n",
            ConditionExpression="attribute_not_exists(#s) OR #s IN (:new,:failed) OR (#s=:p AND claimed_at < :stale)",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":p":"processing", ":r":run_id, ":e":obj["etag"], ":n":now,
                                       ":new":"new", ":failed":"failed", ":stale":now-claim_seconds})
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException": return False
        raise


def parquet_check(key, size):
    if not key.lower().endswith(".parquet"): return False, "not_parquet_extension"
    if size < 8: return False, "file_too_small"
    head = s3.get_object(Bucket=bucket, Key=key, Range="bytes=0-3")["Body"].read()
    tail = s3.get_object(Bucket=bucket, Key=key, Range=f"bytes={size-4}-{size-1}")["Body"].read()
    if head != b"PAR1" or tail != b"PAR1": return False, "missing_parquet_magic"
    return True, None


def handler(event, context):
    if event.get("action") == "acquire_lock":
        now = int(time.time()); run_id = str(uuid.uuid4())
        try:
            locks.put_item(Item={"lock_name":"pipeline", "run_id":run_id,
                                 "execution_id":event["execution_id"], "started_at":now,
                                 "expires_at":now+43200},
                           ConditionExpression="attribute_not_exists(lock_name) OR expires_at < :now",
                           ExpressionAttributeValues={":now":now})
            return {"acquired":True, "run_id":run_id}
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return {"acquired":False, "run_id":run_id}
            raise

    run_id = event["run_id"]
    files, valid, rejected = [], [], []
    pages = s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
    for page in pages:
        for item in page.get("Contents", []):
            key = item["Key"]
            if key.endswith("/"): continue
            obj = {"bucket":bucket, "key":key, "etag":item["ETag"].strip('"'), "size":item["Size"]}
            if not claim(obj, run_id): continue
            files.append(obj)
            try: ok, reason = parquet_check(key, item["Size"])
            except Exception as exc: ok, reason = False, f"preflight_error:{type(exc).__name__}"
            if ok: valid.append(obj)
            else: obj["reason"] = reason; rejected.append(obj)
            if len(files) >= limit: return {"files":files, "valid":valid, "rejected":rejected}
    return {"files":files, "valid":valid, "rejected":rejected}
