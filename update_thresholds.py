import json, boto3
from decimal import Decimal

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('ApplianceThresholds')

def lambda_handler(event, context):

    body = json.loads(event["body"]) if "body" in event else event

    for k, v in body.items():
        table.put_item(Item={
            "appliance": k,
            "threshold": Decimal(str(v))   # now kWh
        })

    return {
        "statusCode": 200,
        "headers": {"Access-Control-Allow-Origin": "*"},
        "body": json.dumps("Thresholds updated in kWh")
    }