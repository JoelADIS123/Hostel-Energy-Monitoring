import json
import boto3
from datetime import datetime

dynamodb = boto3.resource('dynamodb')
active_table = dynamodb.Table('EnergyAlertsTable')
history_table = dynamodb.Table('EnergyAlertsHistoryTable')

def lambda_handler(event, context):
    items = active_table.scan().get("Items", [])
    now = datetime.utcnow().isoformat()

    for item in items:
        alert_id = item["alert_id"]

        # update history record
        history_table.update_item(
            Key={"alert_id": alert_id},
            UpdateExpression="SET is_active = :false, acknowledged_at = :ack",
            ExpressionAttributeValues={
                ":false": False,
                ":ack": now
            }
        )

        # remove from active alerts table
        active_table.delete_item(
            Key={"alert_id": alert_id}
        )

    return {
        "statusCode": 200,
        "body": json.dumps("All active alerts acknowledged and moved to history state")
    }