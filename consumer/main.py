import json
import os
import time
import psycopg2
import paho.mqtt.client as mqtt

MQTT_BROKER = os.getenv("MQTT_BROKER", "mqtt")
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "truck/telemetry")

PGHOST = os.getenv("PGHOST", "postgis")
PGUSER = os.getenv("PGUSER", "postgres")
PGPASSWORD = os.getenv("PGPASSWORD", "postgres")
PGDATABASE = os.getenv("PGDATABASE", "trucks")

def init_db():
    conn = psycopg2.connect(
        host=PGHOST, user=PGUSER, password=PGPASSWORD, dbname=PGDATABASE
    )
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS truck_data (
            id SERIAL PRIMARY KEY,
            truck_id TEXT NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL,
            geom GEOGRAPHY(Point, 4326),
            metrics JSONB
        );
    """)
    conn.commit()
    cur.close()
    conn.close()

def insert_message(payload):
    try:
        conn = psycopg2.connect(
            host=PGHOST, user=PGUSER, password=PGPASSWORD, dbname=PGDATABASE
        )
        cur = conn.cursor()
        truck_id = payload["truck_id"]
        ts = payload["timestamp"]
        lat = payload["gps"]["lat"]
        lon = payload["gps"]["lon"]
        metrics = json.dumps(payload.get("metrics", {}))

        cur.execute("""
            INSERT INTO truck_data (truck_id, timestamp, geom, metrics)
            VALUES (%s, %s, ST_GeogFromText(%s), %s);
        """, (
            truck_id, ts,
            f'POINT({lon} {lat})',
            metrics
        ))

        conn.commit()
        cur.close()
        conn.close()
        print(f"✅ Saved telemetry for {truck_id} at {ts}")
    except Exception as e:
        print(f"❌ Error saving message: {e}")

def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT broker:", rc)
    client.subscribe(MQTT_TOPIC)

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode())
        insert_message(data)
    except Exception as e:
        print(f"❌ Invalid message: {e}")

def main():
    init_db()
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_BROKER, 1883, 60)
    client.loop_forever()

if __name__ == "__main__":
    while True:
        try:
            main()
        except Exception as e:
            print(f"Consumer crashed: {e}, retrying in 5s")
            time.sleep(5)
