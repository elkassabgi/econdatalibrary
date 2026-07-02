import sys, time, os
sys.path.insert(0, r"D:/research/econfindatalibrary/jobs")
import ingest_defillama as L

url = ("https://api.llama.fi/overview/bridge-aggregators?dataType=dailyVolume"
       "&excludeTotalDataChart=true&excludeTotalDataChartBreakdown=false")
d = None
for a in range(8):
    d = L.get(url, timeout=180, tries=2)
    if isinstance(d, dict) and "totalDataChartBreakdown" in d:
        break
    print("retry", a, str(d)[:80], flush=True)
    time.sleep(12)

tdcb = (d or {}).get("totalDataChartBreakdown") or []
keys, dates, vals = [], [], []
for day in tdcb:
    if not isinstance(day, list) or len(day) < 2:
        continue
    od = L.to_date(day[0])
    if od is None:
        continue
    for proto, v in (day[1] or {}).items():
        if isinstance(v, dict):
            v = sum(x for x in v.values() if isinstance(x, (int, float)))
        if isinstance(v, (int, float)):
            keys.append(str(proto)); dates.append(od); vals.append(float(v))

n = L.write_parquet(os.path.join(L.OUT, "bridge_aggregators_dailyVolume.parquet"),
                    {"series_key": keys, "obs_date": dates, "value": vals})
print(f"bridge_aggregators_dailyVolume series={len(set(keys))} obs={n} days={len(tdcb)}",
      flush=True)
