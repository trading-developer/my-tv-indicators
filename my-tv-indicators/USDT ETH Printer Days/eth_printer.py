import requests
import pandas as pd

LIMIT = 1000
API = "https://apilist.tronscanapi.com/api/usdt/turnover"
PARAMS = {"source": "eth", "sort": "desc", "limit": LIMIT, "start": 0}

def fetch_last_data():
    r = requests.get(API, params=PARAMS, timeout=30)
    r.raise_for_status()
    js = r.json()
    return pd.DataFrame(js["data"])

def prepare_events(df):
    df["net_delta"] = pd.to_numeric(df["issue"], errors="coerce") - pd.to_numeric(df["redeem"], errors="coerce")
    df["date"] = pd.to_datetime(df["time"], unit="ms", utc=True).dt.normalize()
    df = df[df["net_delta"] != 0].copy()
    df = df.sort_values("date")
    df = df.tail(LIMIT).reset_index(drop=True)
    # для Pine: timestamp в ms
    out = pd.DataFrame({
        "ts": df["date"].view("int64") // 10**6,
        "net_delta": df["net_delta"]
    })
    return out

if __name__ == "__main__":
    raw = fetch_last_data()
    events = prepare_events(raw)
    events.to_csv("events_for_pine.csv", index=False)

    ts_arr = ",".join(str(int(x)) for x in events["ts"].tolist())
    val_arr = ",".join(str(float(x)) for x in events["net_delta"].tolist())
    print("evTs =", ts_arr)
    print("evDelta =", val_arr)
    print("Saved: events_for_pine.csv")
