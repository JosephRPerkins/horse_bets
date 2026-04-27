import requests
import config

resp = requests.get(
    f"{config.RACING_API_BASE_URL}/racecards/standard",
    auth=(config.RACING_API_USERNAME, config.RACING_API_PASSWORD),
    params={"day": "today"}
)
print(resp.status_code)
data = resp.json()
racecards = data.get("racecards", [])
print(f"Races returned: {len(racecards)}")
if racecards:
    print(f"First race: {racecards[0].get('course')} - {racecards[0].get('off_time')}")
