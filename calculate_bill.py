import boto3

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('HostelEnergyTable')

COST_PER_UNIT = 8  # ₹ per kWh

def lambda_handler(event, context):
    response = table.scan()

    total_units = 0.0
    for item in response['Items']:
        total_units += float(item['usage_kwh'])

    total_bill = round(total_units * COST_PER_UNIT, 2)

    return {
        "statusCode": 200,
        "body": {
            "total_units": round(total_units, 2),
            "total_bill": total_bill
        }
    }