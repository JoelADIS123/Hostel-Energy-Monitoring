import boto3
from decimal import Decimal

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('ApplianceThresholds')

def lambda_handler(event, context):

    items = table.scan()["Items"]

    result = {}
    for i in items:
        val = i["threshold"]
        result[i["appliance"]] = float(val) if isinstance(val, Decimal) else val

    return result