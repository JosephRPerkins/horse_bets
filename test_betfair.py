import requests
import config

resp = requests.post(
    "https://identitysso-cert.betfair.com/api/certlogin",
    data={"username": config.BETFAIR_USERNAME, "password": config.BETFAIR_PASSWORD},
    headers={"X-Application": config.BETFAIR_APP_KEY, "Content-Type": "application/x-www-form-urlencoded"},
    cert=(f"{config.BETFAIR_CERTS_DIR_SERVER}/client-2048.crt", f"{config.BETFAIR_CERTS_DIR_SERVER}/client-2048.key")
)
print(resp.json())
