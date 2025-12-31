# tools/api_consistency_test.py
import time
import statistics
import requests

API = "http://localhost:8000/query"
TOP_K = 3
N = 30

queries = [
    "latest wikimedia edit",
    "what changed recently on wikipedia",
    "wikidata edit about house",
    "anime 2025 edit",
]

lat = []
ok = 0

for i in range(N):
    q = queries[i % len(queries)]
    t0 = time.time()
    r = requests.post(API, json={"query": q, "top_k": TOP_K}, timeout=10)
    dt = time.time() - t0
    lat.append(dt)

    if r.status_code != 200:
        print("FAIL", r.status_code, r.text[:200])
        continue

    data = r.json()
    results = data.get("results", [])
    if isinstance(results, list) and len(results) >= 1:
        ok += 1

print(f"success={ok}/{N}")
print(f"p50={statistics.median(lat):.3f}s p95={sorted(lat)[int(0.95*(len(lat)-1))]:.3f}s max={max(lat):.3f}s")
if ok < int(0.9 * N):
    raise SystemExit("FAIL: consistency below 90%")
print("PASS")
