import json
import boto3
import random
from datetime import datetime
from decimal import Decimal

dynamodb = boto3.resource('dynamodb')
energy_table = dynamodb.Table('HostelEnergyTable')
threshold_table = dynamodb.Table('ApplianceThresholds')
active_alerts_table = dynamodb.Table('EnergyAlertsTable')
alert_history_table = dynamodb.Table('EnergyAlertsHistoryTable')

APPS = ["fan", "ac", "laptop", "tube_light", "mobile_charger"]

def get_thresholds():
    res = threshold_table.scan()
    return {
        i["appliance"]: float(i["threshold"])
        for i in res.get("Items", [])
    }

def status(v, t):
    if v > t:
        return "HIGH"
    elif v < t * 0.5:
        return "LOW"
    return "NORMAL"

def generate_values():
    return {
        "fan": round(random.uniform(0.06, 0.08), 3),
        "ac": round(random.uniform(1.2, 1.8), 3),
        "laptop": round(random.uniform(0.05, 0.10), 3),
        "tube_light": round(random.uniform(0.02, 0.04), 3),
        "mobile_charger": round(random.uniform(0.005, 0.015), 3)
    }

def put_alert(room_id, timestamp, appliance, value, threshold, status_value, message):
    alert_id = f"{room_id}-{appliance}-{timestamp}"

    alert_item = {
        "alert_id": alert_id,
        "room_id": room_id,
        "timestamp": timestamp,
        "appliance": appliance,
        "value": Decimal(str(value)),
        "threshold": Decimal(str(threshold)),
        "status": status_value,
        "message": message,
        "is_active": True
    }

    # active alerts
    active_alerts_table.put_item(Item=alert_item)

    # permanent history
    alert_history_table.put_item(Item={
        **alert_item,
        "acknowledged_at": None
    })

def lambda_handler(event, context):
    thresholds = get_thresholds()

    for i in range(1, 11):
        room_id = str(100 + i)
        timestamp = datetime.utcnow().isoformat()
        values = generate_values()

        item = {
            "room_id": room_id,
            "timestamp": timestamp
        }

        total = 0.0

        for app in APPS:
            value = values[app]
            threshold = thresholds.get(app, 0.1)
            stat = status(value, threshold)

            item[f"{app}_watt"] = Decimal(str(value))
            item[f"{app}_threshold"] = Decimal(str(threshold))
            item[f"{app}_status"] = stat

            if stat == "HIGH":
                put_alert(
                    room_id=room_id,
                    timestamp=timestamp,
                    appliance=app,
                    value=value,
                    threshold=threshold,
                    status_value="HIGH",
                    message=f"{app} usage exceeded threshold"
                )

            total += value

        if random.random() < 0.3:
            extra_value = round(random.uniform(1.0, 2.0), 3)
            item["extra_appliance_watt"] = Decimal(str(extra_value))
            item["extra_appliance_status"] = "UNUSUAL"
            total += extra_value

            put_alert(
                room_id=room_id,
                timestamp=timestamp,
                appliance="extra_appliance",
                value=extra_value,
                threshold=0,
                status_value="UNUSUAL",
                message="Unknown extra appliance detected"
            )

        item["total_watt"] = Decimal(str(round(total, 3)))
        energy_table.put_item(Item=item)

    return {
        "statusCode": 200,
        "body": json.dumps("Data generated with alerts")
    }