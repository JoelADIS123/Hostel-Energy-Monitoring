import boto3
from decimal import Decimal

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('HostelEnergyTable')

def convert(item):
    return {k: float(v) if isinstance(v, Decimal) else v for k, v in item.items()}

def lambda_handler(event, context):

    items = table.scan()["Items"]

    latest = {}
    for i in sorted(items, key=lambda x:x["timestamp"], reverse=True):
        if i["room_id"] not in latest:
            latest[i["room_id"]] = convert(i)

    return list(latest.values())