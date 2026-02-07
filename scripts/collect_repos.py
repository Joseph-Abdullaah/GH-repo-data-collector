import requests, time, json, os, math
import pandas as pd
from datetime import datetime, date
from utils import month_windows, split_window, backoff_sleep, load_env_token
from tqdm import tqdm
from dotenv import load_dotenv

# Load env vars
load_dotenv()

# Load config
ROOT = os.path.dirname(os.path.dirname(__file__))
with open(os.path.join(ROOT, "config.json"), "r") as f:
    CONFIG = json.load(f)

GITHUB_TOKEN_ENV = CONFIG.get("GITHUB_TOKEN_ENV", "GITHUB_TOKEN")
GITHUB_TOKEN = load_env_token(GITHUB_TOKEN_ENV)
HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json",
    "User-Agent": "github-data-collector/1.0"
}

PER_PAGE = CONFIG.get("per_page", 100)
TARGET_ROWS = CONFIG.get("target_rows", 2000)
POLITE_DELAY = CONFIG.get("polite_delay_seconds", 0.5)
MAX_ATTEMPTS = CONFIG.get("max_attempts", 6)
TOTAL_COUNT_SPLIT_THRESHOLD = CONFIG.get("total_count_split_threshold", 900)
EXCLUDE_FORKS = CONFIG.get("exclude_forks", True)
INCLUDE_TOPICS = CONFIG.get("include_topics", True)


SEARCH_URL = "https://api.github.com/search/repositories"

def fetch_search(q, page=1, per_page=100, attempt=0):
    print(f"Fetching {q} page={page}", flush=True)
    params = {"q": q, "page": page, "per_page": per_page}
    for a in range(MAX_ATTEMPTS):
        r = requests.get(SEARCH_URL, params=params, headers=HEADERS, timeout=30)
        if r.status_code == 200:
            return r.json()
        if r.status_code in (429, 403, 500, 502, 503, 504):  # rate limit, abuse, or server error
            print(f"  [Attempt {a+1}/{MAX_ATTEMPTS}] Status {r.status_code}: {r.text[:200]}", flush=True)
            backoff_sleep(a, base=1.0)
            continue
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print(f"Request failed: {e}")
            print(f"Response: {r.text}")
            raise
    raise RuntimeError("Exceeded retries for fetch_search")

def normalize_item(it, include_topics=True):
    # Topics may require a separate call to topics endpoint unless included in search results
    topics = ",".join(it.get("topics", [])) if include_topics else None
    return {
        "repo_id": it.get("id"),
        "full_name": it.get("full_name"),
        "name": it.get("name"),
        "owner": it.get("owner", {}).get("login"),
        "language": it.get("language"),
        "created_at": it.get("created_at"),
        "updated_at": it.get("updated_at"),
        "size_kb": it.get("size"),
        "stargazers_count": it.get("stargazers_count"),
        "forks_count": it.get("forks_count"),
        "open_issues_count": it.get("open_issues_count"),
        "watchers_count": it.get("watchers_count"),
        "license": (it.get("license") or {}).get("name"),
        "topics": topics
    }

def collect(target_rows=TARGET_ROWS, start_date_str=None, end_date_str=None):
    if start_date_str is None:
        start_date_str = CONFIG.get("start_date", "2015-01-01")
    if end_date_str is None:
        end_date_str = CONFIG.get("end_date", datetime.utcnow().date().isoformat())

    start_date = datetime.fromisoformat(start_date_str).date()
    end_date = datetime.fromisoformat(end_date_str).date()

    if CONFIG.get("random_sampling", False):
        import random
        from datetime import timedelta
        sample_size = CONFIG.get("random_sample_size", 50)
        window_days = CONFIG.get("random_window_days", 1)
        queue = []
        total_days = (end_date - start_date).days
        for _ in range(sample_size):
            offset = random.randint(0, total_days)
            s = start_date + timedelta(days=offset)
            e = s + timedelta(days=window_days - 1)
            if e > end_date:
                e = end_date
            queue.append((s, e))
        # Sort for clearer logs
        queue.sort()
    else:
        queue = list(month_windows(start_date, end_date))
    seen = set()
    rows = []

    print(f"Starting collection target={target_rows}, windows={len(queue)}")
    # Use an explicit queue to allow adding split windows when capped
    i = 0
    pbar = tqdm(total=target_rows, desc="Collected")
    while queue and len(rows) < target_rows:
        s, e = queue.pop(0)
        i += 1
        q_date = f"created:{s.isoformat()}..{e.isoformat()}"
        q = q_date + (" fork:false" if EXCLUDE_FORKS else "")
        page = 1
        # fetch first page to inspect total_count
        data = fetch_search(q, page=1, per_page=1)  # minimal page to get total_count
        total_count = data.get("total_count", 0)
        # if total_count is too big (hit 1000 cap risk), split window
        if total_count > TOTAL_COUNT_SPLIT_THRESHOLD and s < e:
            # split window into halves and enqueue both
            w1, w2 = split_window(s, e)
            queue.insert(0, w2)
            queue.insert(0, w1)
            continue

        # otherwise page through
        while True:
            data = fetch_search(q, page=page, per_page=PER_PAGE)
            items = data.get("items", [])
            if not items:
                break
            for it in items:
                rid = it.get("id")
                if rid in seen:
                    continue
                seen.add(rid)
                rows.append(normalize_item(it, include_topics=INCLUDE_TOPICS))
                pbar.update(1)
                if len(rows) >= target_rows:
                    break
            if len(rows) >= target_rows:
                break
            page += 1
            # if pages exceed 10 and total_count==0 something odd; break safety
            time.sleep(POLITE_DELAY)
        # end while pages
    pbar.close()
    print(f"Collected {len(rows)} unique repos.")
    return rows

def save(rows, csv_path, excel_path):
    df = pd.DataFrame(rows)
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    df.to_csv(csv_path, index=False)
    df.to_excel(excel_path, index=False, engine="openpyxl")
    print(f"Saved CSV: {csv_path}")
    print(f"Saved Excel: {excel_path}")

if __name__ == "__main__":
    rows = collect()
    root = os.path.dirname(os.path.dirname(__file__))
    csv_path = os.path.join(root, "data", "github_repos_raw.csv")
    excel_path = os.path.join(root, "data", "github_repos_raw.xlsx")
    save(rows, csv_path, excel_path)
