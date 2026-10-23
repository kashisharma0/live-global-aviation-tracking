import json
import os
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

# ---------- Configuration ----------
TOKEN_URL = ("https://auth.opensky-network.org/auth/realms/"
             "opensky-network/protocol/openid-connect/token")
API_URL = "https://opensky-network.org/api/states/all"

KAFKA_BOOTSTRAP = "localhost:9094"       # the EXTERNAL listener from docker-compose
TOPIC = "flights_raw"
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", 90))   # seconds between API calls

# Names of the 17 positions in an OpenSky state vector
FIELDS = [
    "icao24", "callsign", "origin_country", "time_position", "last_contact",
    "longitude", "latitude", "baro_altitude", "on_ground", "velocity",
    "true_track", "vertical_rate", "sensors", "geo_altitude", "squawk",
    "spi", "position_source",
]


# ---------- OAuth2 token handling ----------
class TokenManager:
    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret
        self.token = None
        self.expires_at = 0

    def get(self):
        # Refresh 60 seconds before the real expiry time
        if self.token is None or time.time() > self.expires_at - 60:
            self._refresh()
        return self.token

    def _refresh(self):
        resp = requests.post(TOKEN_URL, data={
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }, timeout=30)
        resp.raise_for_status()
        body = resp.json()
        self.token = body["access_token"]
        self.expires_at = time.time() + body.get("expires_in", 1800)
        print("[auth] new token obtained")


# ---------- Fetch and clean ----------
def fetch_states(token):
    resp = requests.get(API_URL,
                        headers={"Authorization": f"Bearer {token}"},
                        timeout=60)
    if resp.status_code == 429:
        raise RuntimeError("RATE_LIMIT")
    resp.raise_for_status()
    remaining = resp.headers.get("X-Rate-Limit-Remaining")
    return resp.json(), remaining


def clean_state(raw, api_time, ingest_time):
    """Turn one raw list into a dict. Return None if it is unusable."""
    raw = list(raw) + [None] * (len(FIELDS) - len(raw))   # pad if shorter
    rec = dict(zip(FIELDS, raw))

    if not rec["icao24"] or rec["latitude"] is None or rec["longitude"] is None:
        return None

    rec["callsign"] = (rec["callsign"] or "").strip() or None
    rec.pop("sensors", None)             # not needed
    rec["api_time"] = api_time
    rec["ingest_time"] = ingest_time
    return rec


# ---------- Main loop ----------
def main():
    tokens = TokenManager(os.getenv("OPENSKY_CLIENT_ID"),
                          os.getenv("OPENSKY_CLIENT_SECRET"))

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        linger_ms=50,
    )

    cycle = 0
    while True:
        cycle += 1
        try:
            data, remaining = fetch_states(tokens.get())
            api_time = data.get("time")
            ingest_time = datetime.now(timezone.utc).isoformat()
            states = data.get("states") or []

            sent = dropped = 0
            for raw in states:
                rec = clean_state(raw, api_time, ingest_time)
                if rec is None:
                    dropped += 1
                    continue
                producer.send(TOPIC, key=rec["icao24"], value=rec)
                sent += 1
            producer.flush()

            print(f"[cycle {cycle}] received={len(states)} sent={sent} "
                  f"dropped={dropped} credits_left={remaining}")

        except RuntimeError as e:
            if str(e) == "RATE_LIMIT":
                print("[warn] credits exhausted (429). Sleeping 10 minutes.")
                time.sleep(600)
                continue
            raise
        except Exception as e:
            print(f"[error] {type(e).__name__}: {e}  -> retrying in 30 s")
            time.sleep(30)
            continue

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nProducer stopped.")