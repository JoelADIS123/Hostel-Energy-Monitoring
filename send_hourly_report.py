import json
import boto3
from decimal import Decimal
from datetime import datetime, timedelta, timezone
from collections import defaultdict, Counter

sns = boto3.client("sns")
dynamodb = boto3.resource("dynamodb")

table = dynamodb.Table("HostelEnergyTable")

TOPIC_ARN = "arn:aws:sns:eu-north-1:412902451621:EnergyAlertTopic"
COST_PER_KWH = 8

APPS = ["fan", "ac", "laptop", "tube_light", "mobile_charger"]

ENERGY_FIELDS = {
    "fan": "fan_watt",
    "ac": "ac_watt",
    "laptop": "laptop_watt",
    "tube_light": "tube_light_watt",
    "mobile_charger": "mobile_charger_watt",
}

STATUS_FIELDS = {
    "fan": "fan_status",
    "ac": "ac_status",
    "laptop": "laptop_status",
    "tube_light": "tube_light_status",
    "mobile_charger": "mobile_charger_status",
}

APP_LABELS = {
    "fan": "Fan",
    "ac": "AC",
    "laptop": "Laptop",
    "tube_light": "Tube Light",
    "mobile_charger": "Mobile Charger",
}

def scan_all_items():
    items = []
    response = table.scan()
    items.extend(response.get("Items", []))

    while "LastEvaluatedKey" in response:
        response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        items.extend(response.get("Items", []))

    return items

def convert_item(item):
    return {
        k: float(v) if isinstance(v, Decimal) else v
        for k, v in item.items()
    }

def parse_timestamp(ts):
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"

    dt = datetime.fromisoformat(ts)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)

def average_status(statuses):
    if not statuses:
        return "NO_DATA"

    counts = Counter(statuses)
    high = counts.get("HIGH", 0)
    normal = counts.get("NORMAL", 0)
    low = counts.get("LOW", 0)

    if high >= normal and high >= low and high > 0:
        return "HIGH"
    if normal >= low and normal > 0:
        return "NORMAL"
    if low > 0:
        return "LOW"
    return statuses[-1]

def room_insight(room_total_kwh, room_cost, app_energy):
    top_app = max(app_energy, key=app_energy.get) if app_energy else None
    top_val = app_energy.get(top_app, 0) if top_app else 0

    if room_cost >= 20:
        return f"High hourly cost observed. Main contributor: {top_app} ({top_val:.3f} kWh)."
    if room_total_kwh >= 2.0:
        return f"High room usage observed. Monitor {top_app} first."
    if room_total_kwh <= 0.2:
        return "Very low room usage observed."
    return f"Usage appears stable. Highest contributor: {top_app}."

def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    start_time = now - timedelta(hours=1)

    raw_items = scan_all_items()
    items = [convert_item(i) for i in raw_items]

    window_items = []
    failed_timestamps = 0

    for item in items:
        try:
            ts = parse_timestamp(item["timestamp"])
            if start_time <= ts <= now:
                window_items.append(item)
        except Exception:
            failed_timestamps += 1
            print("TIMESTAMP PARSE FAILED:", item.get("timestamp"))

    report = []
    report.append("Hostel Energy Summary Report")
    report.append("")
    report.append("Aggregation Window")
    report.append(f"From: {start_time.isoformat()}")
    report.append(f"To:   {now.isoformat()}")
    report.append("")
    report.append("This report sums all records found between invocation time minus 1 hour and invocation time.")
    report.append("")

    if not window_items:
        report.append("No records found in the last 1 hour window.")
        report.append(f"Total records scanned: {len(items)}")
        report.append(f"Timestamps that failed parsing: {failed_timestamps}")

        message = "\n".join(report)

        sns.publish(
            TopicArn=TOPIC_ARN,
            Subject="Hostel Energy Report - Last 1 Hour",
            Message=message
        )

        return {
            "statusCode": 200,
            "body": json.dumps("No records in last 1 hour; diagnostic report sent.")
        }

    by_room = defaultdict(list)
    for item in window_items:
        by_room[item["room_id"]].append(item)

    hostel_total_energy = 0.0
    hostel_total_cost = 0.0

    for room_id in sorted(by_room.keys()):
        rows = by_room[room_id]

        report.append(f"Room {room_id}")
        report.append("-" * 40)
        report.append(f"Records included: {len(rows)}")

        room_total = 0.0
        app_energy_totals = {}

        for app in APPS:
            energy_field = ENERGY_FIELDS[app]
            status_field = STATUS_FIELDS[app]

            app_total = sum(float(r.get(energy_field, 0.0)) for r in rows)
            statuses = [r.get(status_field, "NO_DATA") for r in rows if status_field in r]
            avg_stat = average_status(statuses)

            app_energy_totals[app] = app_total
            room_total += app_total

            report.append(
                f"{APP_LABELS[app]}: {app_total:.3f} kWh total | Average Status = {avg_stat}"
            )

        room_cost = room_total * COST_PER_KWH
        hostel_total_energy += room_total
        hostel_total_cost += room_cost

        report.append(f"Room Total Energy (last 1 hour): {room_total:.3f} kWh")
        report.append(f"Room Total Cost (last 1 hour): ₹{room_cost:.2f}")
        report.append(f"Insight: {room_insight(room_total, room_cost, app_energy_totals)}")
        report.append("")

    report.append("=" * 50)
    report.append("Hostel Summary")
    report.append(f"Rooms covered: {len(by_room)}")
    report.append(f"Records included: {len(window_items)}")
    report.append(f"Total Hostel Energy (last 1 hour): {hostel_total_energy:.3f} kWh")
    report.append(f"Total Hostel Cost (last 1 hour): ₹{hostel_total_cost:.2f}")
    report.append("=" * 50)

    message = "\n".join(report)

    sns.publish(
        TopicArn=TOPIC_ARN,
        Subject="Hostel Energy Report - Last 1 Hour",
        Message=message
    )

    return {
        "statusCode": 200,
        "body": json.dumps("Hourly report sent successfully")
    }