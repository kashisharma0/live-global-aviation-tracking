import os
import requests
from dotenv import load_dotenv

load_dotenv()

TOKEN_URL = ("https://auth.opensky-network.org/auth/realms/"
             "opensky-network/protocol/openid-connect/token")
API_URL = "https://opensky-network.org/api/states/all"

# 1. Ask the auth server for an access token
resp = requests.post(TOKEN_URL, data={
    "grant_type": "client_credentials",
    "client_id": os.getenv("OPENSKY_CLIENT_ID"),
    "client_secret": os.getenv("OPENSKY_CLIENT_SECRET"),
}, timeout=30)
resp.raise_for_status()
token = resp.json()["access_token"]
print("Token received, length:", len(token))

# 2. Fetch live aircraft over a small box (cheap in API credits)
params = {"lamin": 45.8, "lomin": 5.9, "lamax": 47.8, "lomax": 10.5}  # Switzerland
data = requests.get(API_URL, params=params,
                    headers={"Authorization": f"Bearer {token}"},
                    timeout=30)
print("Status:", data.status_code)
print("Credits remaining:", data.headers.get("X-Rate-Limit-Remaining"))

states = data.json().get("states") or []
print("Aircraft found:", len(states))
for s in states[:3]:
    print(s)