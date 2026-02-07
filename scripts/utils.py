import time, requests, os
from dateutil.relativedelta import relativedelta
import datetime
from typing import Iterator, Tuple

def month_windows(start_date: datetime.date, end_date: datetime.date) -> Iterator[Tuple[datetime.date, datetime.date]]:
    cur = start_date
    while cur <= end_date:
        nxt = cur + relativedelta(months=1) - datetime.timedelta(days=1)
        if nxt > end_date:
            nxt = end_date
        yield cur, nxt
        cur = cur + relativedelta(months=1)

def split_window(start: datetime.date, end: datetime.date):
    """Return two half windows (start..mid), (mid+1..end)."""
    mid = start + (end - start) // 2
    first_end = mid
    second_start = mid + datetime.timedelta(days=1)
    return (start, first_end), (second_start, end)

def backoff_sleep(attempt: int, base=1.0):
    delay = base * (2 ** attempt)
    time.sleep(delay)

def load_env_token(env_var_name="GITHUB_TOKEN"):
    token = os.environ.get(env_var_name)
    if not token:
        raise RuntimeError(f"Environment variable {env_var_name} missing. Set it before running.")
    return token
