from flask import Flask, jsonify, render_template, request
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime, timedelta
from sqlalchemy import func
from functools import lru_cache
from dotenv import load_dotenv
import threading, json, calendar, logging, os

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger(__name__)

APP_LANG = os.getenv("APP_LANG", "en")
LANG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "lang")
with open(os.path.join(LANG_DIR, f"{APP_LANG}.json"), "r", encoding="utf-8") as f:
    T = json.load(f)

WD_SHORT = T["calendar"]["weekdays_short"]
WD_MAP = dict(zip(["Mon","Tue","Wed","Thu","Fri","Sat","Sun"], WD_SHORT))
MONTHS_SHORT = T["calendar"]["months_short"]
BASELOAD_HOURS = (int(os.getenv("BASELOAD_HOUR_START", 2)), int(os.getenv("BASELOAD_HOUR_END", 5)))
PRICE_KWH = float(os.getenv("PRICE_KWH", 0.226))
CURRENCY = os.getenv("CURRENCY_SYMBOL", "€")
MQTT_HOST = os.getenv("MQTT_HOST", "127.0.0.1")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "sensor")
OBIS_POWER = os.getenv("OBIS_POWER", "1.7.0")
OBIS_ENERGY = os.getenv("OBIS_ENERGY", "1.8.0")
OBIS_ENERGY_DIVISOR = float(os.getenv("OBIS_ENERGY_DIVISOR", 1000))
DB_PATH = os.getenv("DB_PATH", "instance/energy.db")

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = f"sqlite:///{DB_PATH}"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)


class Measurement(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    timestamp = db.Column(db.DateTime, default=datetime.now, index=True)
    power_watt = db.Column(db.Float)
    total_kwh = db.Column(db.Float)


class MeasurementMinute(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    timestamp = db.Column(db.DateTime, index=True)
    power_avg = db.Column(db.Float)
    power_max = db.Column(db.Float)
    power_min = db.Column(db.Float)
    total_kwh = db.Column(db.Float)


class MeasurementHour(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    timestamp = db.Column(db.DateTime, index=True)
    power_avg = db.Column(db.Float)
    power_max = db.Column(db.Float)
    power_min = db.Column(db.Float)
    kwh_used = db.Column(db.Float)


class MeasurementDay(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    timestamp = db.Column(db.DateTime, index=True)
    power_avg = db.Column(db.Float)
    power_max = db.Column(db.Float)
    power_min = db.Column(db.Float)
    kwh_used = db.Column(db.Float)


def format_weekday(dt):
    return WD_MAP.get(dt.strftime("%a"), dt.strftime("%a")[:2]) + "."


def get_period_bounds(period, start_custom=None, end_custom=None):
    now = datetime.now()
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    
    periods = {
        'today': (today, now),
        'yesterday': (today - timedelta(days=1), today),
        'week': (today - timedelta(days=7), now),
        'month': (now.replace(day=1, hour=0, minute=0, second=0, microsecond=0), now),
        'lastmonth': ((now.replace(day=1) - timedelta(days=1)).replace(day=1, hour=0, minute=0, second=0, microsecond=0), now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)),
        'year': (now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0), now),
    }
    
    if period == 'custom' and start_custom and end_custom:
        return start_custom, end_custom
    
    return periods.get(period, periods['today'])


def get_kwh_for_range(start, end):
    """Berechnet kWh für Zeitraum [start, end) - kombiniert alle Quellen chronologisch."""
    total = 0.0
    cursor = start
    
    day_kwh = db.session.query(func.sum(MeasurementDay.kwh_used)).filter(
        MeasurementDay.timestamp >= start, MeasurementDay.timestamp < end
    ).scalar() or 0
    total += day_kwh
    
    day_last = MeasurementDay.query.filter(
        MeasurementDay.timestamp >= start, MeasurementDay.timestamp < end
    ).order_by(MeasurementDay.timestamp.desc()).first()
    cursor = day_last.timestamp + timedelta(days=1) if day_last else start
    
    hour_kwh = db.session.query(func.sum(MeasurementHour.kwh_used)).filter(
        MeasurementHour.timestamp >= cursor, MeasurementHour.timestamp < end
    ).scalar() or 0
    total += hour_kwh
    
    hour_last = MeasurementHour.query.filter(
        MeasurementHour.timestamp >= cursor, MeasurementHour.timestamp < end
    ).order_by(MeasurementHour.timestamp.desc()).first()
    cursor = hour_last.timestamp + timedelta(hours=1) if hour_last else cursor
    
    minute_first = MeasurementMinute.query.filter(
        MeasurementMinute.timestamp >= cursor, MeasurementMinute.timestamp < end
    ).order_by(MeasurementMinute.timestamp.asc()).first()
    minute_last = MeasurementMinute.query.filter(
        MeasurementMinute.timestamp >= cursor, MeasurementMinute.timestamp < end
    ).order_by(MeasurementMinute.timestamp.desc()).first()
    
    if minute_first and minute_last and minute_first.id != minute_last.id:
        total += minute_last.total_kwh - minute_first.total_kwh
        cursor = minute_last.timestamp + timedelta(minutes=1)
    elif minute_last:
        cursor = minute_last.timestamp + timedelta(minutes=1)
    
    raw_first = Measurement.query.filter(
        Measurement.timestamp >= cursor, Measurement.timestamp < end
    ).order_by(Measurement.timestamp.asc()).first()
    raw_last = Measurement.query.filter(
        Measurement.timestamp >= cursor, Measurement.timestamp < end
    ).order_by(Measurement.timestamp.desc()).first()
    
    if raw_first and raw_last and raw_first.id != raw_last.id:
        total += raw_last.total_kwh - raw_first.total_kwh
    
    return total


def get_history_data(start, end, resolution):
    """Holt History-Daten mit automatischer Quellenauswahl."""
    data = []
    
    if resolution == 'raw':
        rows = Measurement.query.filter(
            Measurement.timestamp >= start, Measurement.timestamp < end
        ).order_by(Measurement.timestamp.asc()).all()
        data = [(r.timestamp, r.power_watt) for r in rows]
    
    elif resolution == 'minute':
        combined = {}
        for m in MeasurementMinute.query.filter(
            MeasurementMinute.timestamp >= start, MeasurementMinute.timestamp < end
        ).all():
            combined[m.timestamp.strftime("%Y-%m-%d %H:%M")] = (m.timestamp, m.power_avg)
        
        for m in Measurement.query.filter(
            Measurement.timestamp >= start, Measurement.timestamp < end
        ).all():
            key = m.timestamp.strftime("%Y-%m-%d %H:%M")
            if key not in combined:
                combined[key] = (m.timestamp, m.power_watt)
        
        data = sorted(combined.values(), key=lambda x: x[0])
    
    elif resolution == 'hour':
        combined = {}
        for m in MeasurementHour.query.filter(
            MeasurementHour.timestamp >= start, MeasurementHour.timestamp < end
        ).all():
            combined[m.timestamp.strftime("%Y-%m-%d %H")] = (m.timestamp, m.power_avg)
        
        for m in MeasurementMinute.query.filter(
            MeasurementMinute.timestamp >= start, MeasurementMinute.timestamp < end
        ).all():
            key = m.timestamp.strftime("%Y-%m-%d %H")
            if key not in combined:
                combined[key] = (m.timestamp, m.power_avg)
        
        for m in Measurement.query.filter(
            Measurement.timestamp >= start, Measurement.timestamp < end
        ).all():
            key = m.timestamp.strftime("%Y-%m-%d %H")
            if key not in combined:
                combined[key] = (m.timestamp, m.power_watt)
        
        data = sorted(combined.values(), key=lambda x: x[0])
    
    elif resolution == 'day':
        combined = {}
        for m in MeasurementDay.query.filter(
            MeasurementDay.timestamp >= start, MeasurementDay.timestamp < end
        ).all():
            combined[m.timestamp.strftime("%Y-%m-%d")] = (m.timestamp, m.power_avg)
        
        for m in MeasurementHour.query.filter(
            MeasurementHour.timestamp >= start, MeasurementHour.timestamp < end
        ).all():
            key = m.timestamp.strftime("%Y-%m-%d")
            if key not in combined:
                combined[key] = (m.timestamp, m.power_avg)
        
        data = sorted(combined.values(), key=lambda x: x[0])
    
    return data


def sample_data(data, max_points=300):
    if len(data) <= max_points:
        return data
    step = max(1, len(data) // max_points)
    return data[::step]


try:
    import paho.mqtt.client as mqtt
    
    def on_connect(client, userdata, flags, rc, properties=None):
        log.info(f"MQTT connected: {rc}")
        client.subscribe(MQTT_TOPIC)
    
    def on_message(client, userdata, msg):
        try:
            data = json.loads(msg.payload.decode())
            watt = float(data.get(OBIS_POWER, 0))
            kwh = float(data.get(OBIS_ENERGY, 0)) / OBIS_ENERGY_DIVISOR
            with app.app_context():
                db.session.add(Measurement(power_watt=watt, total_kwh=kwh))
                db.session.commit()
        except Exception as e:
            log.error(f"MQTT message error: {e}")
    
    def start_mqtt():
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        client.on_connect = on_connect
        client.on_message = on_message
        client.connect(MQTT_HOST, MQTT_PORT, 60)
        client.loop_forever()
    
    MQTT_AVAILABLE = True
except ImportError:
    log.warning("paho-mqtt not installed, MQTT disabled")
    MQTT_AVAILABLE = False


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/i18n")
def api_i18n():
    out = dict(T)
    out["currency"] = CURRENCY
    return jsonify(out)


@app.route("/api/latest")
def api_latest():
    m = Measurement.query.order_by(Measurement.id.desc()).first()
    if not m:
        return jsonify({"power_watt": 0, "total_kwh": 0, "timestamp": None, "kwh_today": 0, "online": False})
    
    now = datetime.now()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    first_today = Measurement.query.filter(Measurement.timestamp >= today_start).order_by(Measurement.id.asc()).first()
    kwh_today = m.total_kwh - first_today.total_kwh if first_today else 0
    online = (now - m.timestamp).total_seconds() < 30
    
    return jsonify({
        "power_watt": m.power_watt,
        "total_kwh": round(m.total_kwh, 2),
        "kwh_today": round(kwh_today, 2),
        "timestamp": m.timestamp.isoformat(),
        "online": online,
        "last_seen": m.timestamp.strftime("%H:%M:%S")
    })


@app.route("/api/gauge-range")
def api_gauge_range():
    week_ago = datetime.now() - timedelta(days=7)
    
    peaks = [
        db.session.query(func.max(MeasurementHour.power_max)).filter(MeasurementHour.timestamp >= week_ago).scalar(),
        db.session.query(func.max(MeasurementMinute.power_max)).filter(MeasurementMinute.timestamp >= week_ago).scalar(),
        db.session.query(func.max(Measurement.power_watt)).filter(Measurement.timestamp >= week_ago).scalar(),
    ]
    peak = max(filter(None, peaks), default=1000)
    gauge_max = int((peak // 1000 + 1) * 1000)
    
    return jsonify({
        "peak_7d": round(peak, 0),
        "gauge_max": gauge_max,
        "zone_green": int(gauge_max * 0.15),
        "zone_yellow": int(gauge_max * 0.35),
        "zone_orange": int(gauge_max * 0.60)
    })


@app.route("/api/stats")
def api_stats():
    now = datetime.now()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_start = today_start - timedelta(days=1)
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    prev_month_end = month_start
    prev_month_start = (month_start - timedelta(days=1)).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    
    kwh_today = get_kwh_for_range(today_start, now)
    kwh_yesterday = get_kwh_for_range(yesterday_start, today_start)
    kwh_month = get_kwh_for_range(month_start, now)
    kwh_prev_month = get_kwh_for_range(prev_month_start, prev_month_end)
    
    if kwh_month > 0:
        hours_in_month = (now - month_start).total_seconds() / 3600
        if hours_in_month > 0:
            kwh_per_hour = kwh_month / hours_in_month
            days_total = calendar.monthrange(now.year, now.month)[1]
            hours_remaining = (days_total * 24) - hours_in_month
            prognosis_month = kwh_month + (kwh_per_hour * hours_remaining)
        else:
            prognosis_month = 0
    else:
        prognosis_month = 0
    
    if kwh_prev_month > 0:
        days_prev = calendar.monthrange(prev_month_start.year, prev_month_start.month)[1]
        daily_avg_prev = kwh_prev_month / days_prev
        expected = daily_avg_prev * now.day
        month_change_pct = ((kwh_month / expected) - 1) * 100 if expected > 0 else 0
    else:
        month_change_pct = 0
    
    baseload = db.session.query(func.min(Measurement.power_watt)).filter(
        Measurement.timestamp >= yesterday_start,
        func.cast(func.strftime('%H', Measurement.timestamp), db.Integer).between(*BASELOAD_HOURS)
    ).scalar() or 0
    
    return jsonify({
        "kwh_today": round(kwh_today, 2),
        "kwh_yesterday": round(kwh_yesterday, 2),
        "kwh_month": round(kwh_month, 2),
        "kwh_prev_month": round(kwh_prev_month, 2),
        "month_change_pct": round(month_change_pct, 1),
        "prognosis_month": round(prognosis_month, 1),
        "prognosis_cost": round(prognosis_month * PRICE_KWH, 2),
        "cost_month": round(kwh_month * PRICE_KWH, 2),
        "baseload_watt": round(baseload, 0),
        "price_kwh": PRICE_KWH
    })


@app.route("/api/history")
def api_history():
    period = request.args.get("period", "today")
    now = datetime.now()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    
    start_custom = end_custom = None
    if period == "custom":
        try:
            start_custom = datetime.fromisoformat(request.args.get("start"))
            end_custom = datetime.fromisoformat(request.args.get("end")) + timedelta(days=1)
        except:
            return jsonify({"labels": [], "data": [], "period": "custom", "error": "Invalid date"})
    
    start, end = get_period_bounds(period, start_custom, end_custom)
    days_span = (end - start).days + ((end - start).seconds > 0)
    
    if period in ('today', 'yesterday') or (period == 'custom' and days_span <= 2):
        return _history_hours(start, end, now, period)
    elif period == 'year' or (period == 'custom' and days_span >= 49):
        return _history_months(start, end, now, period)
    else:
        return _history_days(start, end, now, period)


def _history_hours(start, end, now, period):
    bars = []
    cursor = start.replace(minute=0, second=0, microsecond=0)
    has_data = False
    days_span = (end - start).days + ((end - start).seconds > 0)
    multi_day = days_span > 1
    while cursor < end:
        next_h = cursor + timedelta(hours=1)
        bound = min(next_h, end)
        data = get_history_data(cursor, bound, 'minute')
        if data:
            avg_w = sum(v for _, v in data) / len(data)
        else:
            raw = get_history_data(cursor, bound, 'raw')
            avg_w = sum(v for _, v in raw) / len(raw) if raw else 0
        if avg_w > 0:
            has_data = True
        if avg_w > 0 or cursor < now:
            wd = WD_MAP.get(cursor.strftime("%a"), cursor.strftime("%a")[:2])
            tooltip = f"{wd} {cursor.strftime('%d.%m.')} {cursor.strftime('%H:%M')}"
            bars.append({"label": cursor.strftime("%H:%M"), "tooltip": tooltip, "value": round(avg_w, 1), "is_weekend": False})
        cursor = next_h
    
    non_zero = sum(1 for b in bars if b["value"] > 0)
    if non_zero < 2 and has_data is False:
        hour_rows = MeasurementHour.query.filter(
            MeasurementHour.timestamp >= start, MeasurementHour.timestamp < end
        ).order_by(MeasurementHour.timestamp.asc()).all()
        if hour_rows:
            bars = []
            for r in hour_rows:
                wd = WD_MAP.get(r.timestamp.strftime("%a"), r.timestamp.strftime("%a")[:2])
                tooltip = f"{wd} {r.timestamp.strftime('%d.%m.')} {r.timestamp.strftime('%H:%M')}"
                bars.append({"label": r.timestamp.strftime("%H:%M"), "tooltip": tooltip, "value": round(r.power_avg, 1), "is_weekend": False})
            has_data = True
    
    if not has_data or sum(1 for b in bars if b["value"] > 0) < 2:
        return _history_days(start, end, now, period)
    
    avg = sum(b["value"] for b in bars) / len(bars) if bars else 0
    return jsonify({
        "labels": [b["label"] for b in bars],
        "tooltips": [b["tooltip"] for b in bars],
        "data": [b["value"] for b in bars],
        "period": period, "chart_type": "bar", "bar_unit": "watt",
        "is_weekend": [False] * len(bars),
        "avg_kwh": round(avg, 1)
    })


def _history_days(start, end, now, period):
    bars = []
    cursor = start.replace(hour=0, minute=0, second=0, microsecond=0)
    while cursor < end:
        next_day = cursor + timedelta(days=1)
        kwh = get_kwh_for_range(cursor, min(next_day, end))
        if kwh > 0 or cursor < now:
            wd_en = cursor.strftime("%a")
            wd_de = WD_MAP.get(wd_en, wd_en[:2])
            bars.append({
                "label": f"{wd_de} {cursor.strftime('%d.%m.')}",
                "value": round(kwh, 3),
                "is_weekend": wd_en in ("Sat", "Sun")
            })
        cursor = next_day
    avg = sum(b["value"] for b in bars) / len(bars) if bars else 0
    return jsonify({
        "labels": [b["label"] for b in bars],
        "data": [b["value"] for b in bars],
        "period": period, "chart_type": "bar", "bar_unit": "kwh",
        "is_weekend": [b["is_weekend"] for b in bars],
        "avg_kwh": round(avg, 2)
    })


def _history_months(start, end, now, period):
    bars = []
    cursor = start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    while cursor < end:
        y, m = cursor.year, cursor.month
        next_m = (cursor.replace(day=28) + timedelta(days=4)).replace(day=1)
        kwh = get_kwh_for_range(cursor, min(next_m, end))
        if kwh > 0 or cursor < now:
            bars.append({
                "label": f"{MONTHS_SHORT[m-1]} {y}" if (end - start).days > 365 else MONTHS_SHORT[m-1],
                "value": round(kwh, 2),
                "is_weekend": False
            })
        cursor = next_m
    avg = sum(b["value"] for b in bars) / len(bars) if bars else 0
    return jsonify({
        "labels": [b["label"] for b in bars],
        "data": [b["value"] for b in bars],
        "period": period, "chart_type": "bar", "bar_unit": "kwh",
        "is_weekend": [False] * len(bars),
        "avg_kwh": round(avg, 2)
    })


@app.route("/api/stats-range")
def api_stats_range():
    period = request.args.get("period", "today")
    
    start_custom = end_custom = None
    if period == "custom":
        try:
            start_custom = datetime.fromisoformat(request.args.get("start"))
            end_custom = datetime.fromisoformat(request.args.get("end")) + timedelta(days=1)
        except:
            return jsonify({"kwh": 0, "cost": 0, "period": "custom", "change_pct": None, "prev_label": ""})
    
    start, end = get_period_bounds(period, start_custom, end_custom)
    kwh = get_kwh_for_range(start, end)
    
    span = end - start
    prev_end = start
    prev_start = prev_end - span
    
    prev_labels = T.get("prev_period", {})
    
    kwh_prev = get_kwh_for_range(prev_start, prev_end)
    change_pct = round(((kwh / kwh_prev) - 1) * 100, 1) if kwh_prev > 0 else None
    
    return jsonify({
        "kwh": round(kwh, 2),
        "cost": round(kwh * PRICE_KWH, 2),
        "period": period,
        "change_pct": change_pct,
        "prev_label": prev_labels.get(period, 'Vorzeitraum')
    })


if __name__ == "__main__":
    with app.app_context():
        db.create_all()
    
    if MQTT_AVAILABLE:
        threading.Thread(target=start_mqtt, daemon=True).start()
    
    app.run(
        host=os.getenv("FLASK_HOST", "0.0.0.0"),
        port=int(os.getenv("FLASK_PORT", 5000)),
        debug=os.getenv("FLASK_DEBUG", "false").lower() == "true"
    )
