import os, time
import boto3

table = boto3.resource("dynamodb").Table(os.environ["STATE_TABLE"])


def handler(event, context):
    items = event.get("files", [])
    now = int(time.time())
    for obj in items:
        table.update_item(Key={"object_id":f"{obj['bucket']}#{obj['key']}"},
                          UpdateExpression="SET #s=:done, updated_at=:n REMOVE reject_reason",
                          ExpressionAttributeNames={"#s":"status"},
                          ExpressionAttributeValues={":done":"processed", ":n":now})
    return {"count":len(items)}
