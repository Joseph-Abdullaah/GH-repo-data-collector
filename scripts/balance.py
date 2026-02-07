import pandas as pd
import json, os
ROOT = os.path.dirname(os.path.dirname(__file__))
with open(os.path.join(ROOT, "config.json"), "r") as f:
    CONFIG = json.load(f)

star_bins = CONFIG.get("star_bins", [10, 100, 500, 5000])
target_rows = CONFIG.get("target_rows", 2000)
csv_in = os.path.join(ROOT, "data", "github_repos_raw.csv")
df = pd.read_csv(csv_in)
# define bins: [0-10), [10-100), ...
bins = [-1] + star_bins + [10**9]
labels = []
for i in range(len(bins)-1):
    labels.append(f"{bins[i]+1 if bins[i]>=0 else 0}-{bins[i+1]}")
df["star_bin"] = pd.cut(df["stargazers_count"].fillna(0), bins=bins, labels=labels, right=True)

# compute per-bin quotas as equal split
unique_bins = df["star_bin"].dropna().unique().tolist()
per_bin = max(1, target_rows // len(unique_bins))

samples = []
for b in unique_bins:
    sub = df[df["star_bin"] == b]
    if len(sub) <= per_bin:
        samples.append(sub)
    else:
        samples.append(sub.sample(n=per_bin, random_state=42))
final = pd.concat(samples, ignore_index=True)
# if we lack rows because some bins small, top up randomly
if len(final) < target_rows:
    extra = df[~df["repo_id"].isin(final["repo_id"])].sample(n=(target_rows - len(final)), random_state=1)
    final = pd.concat([final, extra], ignore_index=True)

out_csv = os.path.join(ROOT, "data", "github_repos_balanced.csv")
out_xlsx = os.path.join(ROOT, "data", "github_repos_balanced.xlsx")
final.to_csv(out_csv, index=False)
final.to_excel(out_xlsx, index=False, engine="openpyxl")
print("Saved balanced dataset:", out_csv, out_xlsx)
