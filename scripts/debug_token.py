import os
import requests
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("GITHUB_TOKEN")
if not TOKEN:
    print("No GITHUB_TOKEN found.")
    exit(1)

print(f"Token found: {TOKEN[:4]}...{TOKEN[-4:]}")

url = "https://api.github.com/rate_limit"
headers = {
    "Authorization": f"token {TOKEN}",
    "Accept": "application/vnd.github+json",
}

print(f"Checking {url}...")
try:
    r = requests.get(url, headers=headers)
    print(f"Status: {r.status_code}")
    if r.status_code == 200:
        data = r.json()
        core = data.get("resources", {}).get("core", {})
        search = data.get("resources", {}).get("search", {})
        print("Core Rate Limit:")
        print(f"  Limit: {core.get('limit')}")
        print(f"  Remaining: {core.get('remaining')}")
        print(f"  Reset: {core.get('reset')}")
        print("Search Rate Limit:")
        print(f"  Limit: {search.get('limit')}")
        print(f"  Remaining: {search.get('remaining')}")
        print(f"  Reset: {search.get('reset')}")
    else:
        print("Response body:", r.text)
        print("Headers:", dict(r.headers))
except Exception as e:
    print(f"Error: {e}")
