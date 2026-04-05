import json
import boto3
import math
from decimal import Decimal

dynamodb = boto3.resource('dynamodb')
energy_table = dynamodb.Table('HostelEnergyTable')
threshold_table = dynamodb.Table('ApplianceThresholds')

APPS = ["fan", "ac", "laptop", "tube_light", "mobile_charger"]
COST = 8


def convert(i):
    return {k: float(v) if isinstance(v, Decimal) else v for k, v in i.items()}


def get_thr():
    items = threshold_table.scan().get("Items", [])
    return {i["appliance"]: float(i["threshold"]) for i in items}


def mean(a):
    return sum(a) / len(a) if a else 0.0


def std(a):
    if not a:
        return 0.1
    m = mean(a)
    return math.sqrt(sum((x - m) ** 2 for x in a) / len(a))


def hw(s, a=0.5, b=0.3):
    if not s:
        return 0.0
    if len(s) == 1:
        return s[0]

    l = s[0]
    t = s[1] - s[0]

    for i in range(1, len(s)):
        p = l
        l = a * s[i] + (1 - a) * (l + t)
        t = b * (l - p) + (1 - b) * t

    return max(l + t, 0.0)


def lr(s):
    n = len(s)
    if n == 0:
        return 0.0
    if n == 1:
        return s[0]

    x = list(range(n))
    xm = sum(x) / n
    ym = sum(s) / n
    num = sum((x[i] - xm) * (s[i] - ym) for i in x)
    den = sum((x[i] - xm) ** 2 for i in x)
    slope = (num / den) if den else 0
    return max(ym + slope * n, 0.0)


def prob(predicted_value, threshold_value, sigma):
    sigma = max(float(sigma or 0), 0.0001)
    z = (threshold_value - predicted_value) / sigma
    return max(0.0, min(1.0, 1 - (0.5 * (1 + math.erf(z / math.sqrt(2))))))


def recommend(app):
    return {
        "fan": "Reduce speed / turn off when idle",
        "ac": "Increase temp by 1–2°C or reduce runtime",
        "laptop": "Enable battery saver / reduce charging time",
        "tube_light": "Turn off unused lights / use LED",
        "mobile_charger": "Unplug idle chargers"
    }.get(app, "Monitor usage")


def insight(room):
    apps = room["appliances"]
    total = room["total"]

    ranked = sorted(
        apps.items(),
        key=lambda x: (x[1]["prob_exceed_1h"] + x[1]["prob_exceed_24h"]),
        reverse=True
    )

    top = ranked[0]
    app = top[0]
    v = top[1]

    if total["prob_budget_24h"] > 0.8:
        return f"High 24h cost risk. {app} main driver. {recommend(app)}"

    if total["prob_budget_1h"] > 0.7:
        return f"Immediate cost spike likely. {app} dominant. {recommend(app)}"

    if v["prob_exceed_24h"] > 0.7:
        return f"{app} will exceed threshold over time. {recommend(app)}"

    if v["prob_exceed_1h"] > 0.6:
        return f"Short-term spike from {app}. {recommend(app)}"

    return f"Stable usage. Monitor {app}."


def parse_event_body(event):
    if not event:
        return {}

    if isinstance(event, dict) and "body" in event:
        body = event["body"]
        if isinstance(body, str):
            try:
                return json.loads(body)
            except Exception:
                return {}
        if isinstance(body, dict):
            return body

    return event if isinstance(event, dict) else {}


def lambda_handler(event, context):
    body = parse_event_body(event)

    budget_1h = float(body.get("budget_1h", 50))
    budget_24h = float(body.get("budget_24h", 1000))

    items = [convert(i) for i in energy_table.scan().get("Items", [])]
    thr = get_thr()

    rooms = {}
    for i in items:
        rooms.setdefault(i["room_id"], []).append(i)

    out = []

    for r, data in rooms.items():
        data = sorted(data, key=lambda x: x["timestamp"])[-20:]
        res = {"room_id": r, "appliances": {}}

        tot1 = 0.0
        tot24 = 0.0
        appliance_sigmas = []

        for a in APPS:
            s = [float(d.get(f"{a}_watt", 0)) for d in data]

            if len(s) < 6:
                p1 = lr(s)
            else:
                p1 = hw(s)

            temp = s[:] if s else [0.0]
            future = []
            for _ in range(24):
                n = hw(temp)
                n = max(n, 0.0)
                future.append(n)
                temp.append(n)

            p24 = sum(future)

            rsd = [s[i] - s[i - 1] for i in range(1, len(s))]
            sigma_app = max(std(rsd), 0.05)

            appliance_sigmas.append(sigma_app)

            t = float(thr.get(a, 0.1))

            res["appliances"][a] = {
                "pred_1h": round(p1, 3),
                "pred_24h": round(p24, 3),
                "prob_exceed_1h": round(prob(p1, t, sigma_app), 4),
                "prob_exceed_24h": round(prob(p24, t * 24, sigma_app * math.sqrt(24)), 4)
            }

            tot1 += p1
            tot24 += p24

        cost1 = tot1 * COST
        cost24 = tot24 * COST

        total_sigma_energy_1h = math.sqrt(sum(s ** 2 for s in appliance_sigmas))
        total_sigma_energy_24h = total_sigma_energy_1h * math.sqrt(24)

        total_sigma_cost_1h = max(total_sigma_energy_1h * COST, 0.1)
        total_sigma_cost_24h = max(total_sigma_energy_24h * COST, 0.1)

        res["total"] = {
            "pred_1h": round(tot1, 3),
            "pred_24h": round(tot24, 3),
            "cost_1h": round(cost1, 2),
            "cost_24h": round(cost24, 2),
            "prob_budget_1h": round(prob(cost1, budget_1h, total_sigma_cost_1h), 4),
            "prob_budget_24h": round(prob(cost24, budget_24h, total_sigma_cost_24h), 4)
        }

        res["insight"] = insight(res)
        out.append(res)

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*"
        },
        "body": json.dumps(out)
    }