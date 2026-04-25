"""
fetch_data.py
-------------
Pulls all raw data for the Gasly vs Ocon analysis and saves to CSV.
Run once (or re-run to top up missing rounds — laps_raw is resumable).

Usage:
    python3 fetch_data.py
    python3 fetch_data.py --skip-ff1   # Jolpica only (fast)
"""

import argparse
import time
from pathlib import Path

import fastf1
import pandas as pd
import requests

from utils import ALPINE_YEARS, DRIVERS, DATA_DIR, MECHANICAL, INCIDENT, STREET_CIRCUITS, categorise

# ── config ────────────────────────────────────────────────────────────────────
BASE      = "https://api.jolpi.ca/ergast/f1"
CACHE_DIR = DATA_DIR / "ff1_cache"
CACHE_DIR.mkdir(exist_ok=True)
fastf1.Cache.enable_cache(str(CACHE_DIR))


# ── helpers ───────────────────────────────────────────────────────────────────

def jolpica_paginate(path):
    results, offset, limit = [], 0, 100
    while True:
        url = f"{BASE}{path}.json?limit={limit}&offset={offset}"
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        data = r.json()["MRData"]
        total = int(data["total"])
        table = next(v for v in data.values() if isinstance(v, dict))
        records = next(v for v in table.values() if isinstance(v, list))
        results.extend(records)
        offset += limit
        if offset >= total:
            break
        time.sleep(0.1)
    return results


def get_schedule(year):
    data = requests.get(f"{BASE}/{year}.json", timeout=15).json()
    races = data["MRData"]["RaceTable"]["Races"]
    return [(int(r["round"]), r["raceName"]) for r in races]


# ── 1. career results (Jolpica) ───────────────────────────────────────────────
def fetch_career_results(driver_id, abbr):
    print(f"  Fetching {abbr} career results...", end=" ")
    races = jolpica_paginate(f"/drivers/{driver_id}/results")
    rows = []
    for race in races:
        res = race["Results"][0]
        rows.append({
            "driver":      abbr,
            "year":        int(race["season"]),
            "round":       int(race["round"]),
            "race_name":   race["raceName"],
            "date":        pd.to_datetime(race["date"]),
            "constructor": res["Constructor"]["name"],
            "grid":        int(res["grid"]),
            "finish_pos":  int(res["position"]) if res["positionText"].isdigit() else None,
            "points":      float(res["points"]),
            "status":      res["status"],
            "status_cat":  categorise(res["status"]),
            "laps":        int(res["laps"]),
            "is_street":   int(race["raceName"] in STREET_CIRCUITS),
        })
    df = pd.DataFrame(rows)
    print(f"{len(df)} races across {df['year'].nunique()} seasons")
    return df


def fetch_sprint_points(driver_id, abbr):
    print(f"  Fetching {abbr} sprint points...", end=" ")
    years = range(2021, pd.Timestamp.now().year + 1)
    rows = []
    for year in years:
        r = requests.get(f"{BASE}/{year}/drivers/{driver_id}/sprint.json?limit=100", timeout=15)
        r.raise_for_status()
        races = r.json()["MRData"]["RaceTable"]["Races"]
        for race in races:
            res = race["SprintResults"][0]
            rows.append({
                "driver": abbr,
                "year":   int(race["season"]),
                "round":  int(race["round"]),
                "sprint_points": float(res["points"]),
            })
        time.sleep(0.1)
    df = pd.DataFrame(rows)
    total = df["sprint_points"].sum() if not df.empty else 0
    print(f"{total:.0f} sprint pts across {len(df)} sprints")
    return df


def run_career(out_path=None):
    out_path = out_path or DATA_DIR / "career_results.csv"
    print("\n── Career results (Jolpica) ──")
    gas = fetch_career_results("gasly", "GAS")
    oco = fetch_career_results("ocon",  "OCO")
    career = pd.concat([gas, oco], ignore_index=True)

    print("\n── Sprint points (Jolpica) ──")
    gas_s = fetch_sprint_points("gasly", "GAS")
    oco_s = fetch_sprint_points("ocon",  "OCO")
    sprint = pd.concat([gas_s, oco_s], ignore_index=True)

    career = career.merge(sprint, on=["driver", "year", "round"], how="left")
    career["sprint_points"] = career["sprint_points"].fillna(0)

    career.to_csv(out_path, index=False)
    print(f"  Saved {out_path}")
    return career


# ── 2. qualifying H2H (FastF1) ───────────────────────────────────────────────
def extract_quali_session(year, round_num, race_name):
    try:
        session = fastf1.get_session(year, round_num, "Q")
        session.load(laps=False, telemetry=False, weather=False, messages=False)
        results = session.results
        if results is None or results.empty:
            return None
        rows = {}
        for abbr in ["GAS", "OCO"]:
            row = results[results["Abbreviation"] == abbr]
            if not row.empty:
                rows[abbr] = row.iloc[0]
        if len(rows) < 2:
            return None
        gas_time_s = oco_time_s = segment = None
        for seg in ["Q3", "Q2", "Q1"]:
            t_gas = rows["GAS"].get(seg)
            t_oco = rows["OCO"].get(seg)
            if pd.notna(t_gas) and pd.notna(t_oco):
                gas_time_s = t_gas.total_seconds()
                oco_time_s = t_oco.total_seconds()
                segment = seg
                break
        if gas_time_s is None:
            return None
        delta = gas_time_s - oco_time_s
        return {
            "year":       year,
            "round":      round_num,
            "race_name":  race_name,
            "segment":    segment,
            "gas_time_s": gas_time_s,
            "oco_time_s": oco_time_s,
            "delta_s":    delta,
            "faster":     "GAS" if delta < 0 else "OCO",
            "is_street":  int(race_name in STREET_CIRCUITS),
            "gas_grid":   int(rows["GAS"]["Position"]) if pd.notna(rows["GAS"].get("Position")) else None,
            "oco_grid":   int(rows["OCO"]["Position"]) if pd.notna(rows["OCO"].get("Position")) else None,
        }
    except Exception as e:
        print(f"    [{year} R{round_num}] skipped: {e}")
        return None


def run_qualifying(out_path=None):
    out_path = out_path or DATA_DIR / "quali_h2h.csv"
    print("\n── Qualifying H2H (FastF1) ──")
    rows = []
    tenure_race = 0
    for year in ALPINE_YEARS:
        schedule = get_schedule(year)
        print(f"  {year} — {len(schedule)} rounds")
        for rnd, name in schedule:
            tenure_race += 1
            print(f"    Q {year} R{rnd} {name[:30]}...", end=" ")
            row = extract_quali_session(year, rnd, name)
            if row:
                row["tenure_race"] = tenure_race
                row["season_2024"] = int(year == 2024)
                rows.append(row)
                print(f"[{row['segment']}] D={row['delta_s']:+.3f}s")
            else:
                print("no data")
    df = pd.DataFrame(rows)
    df.to_csv(out_path, index=False)
    print(f"  Saved {out_path} — {len(df)} sessions")
    return df


# ── 3. raw laps (FastF1, resumable) ──────────────────────────────────────────
RAW_COLS = ["Driver", "Compound", "Stint", "LapNumber", "LapTime", "PitInTime", "TrackStatus"]

def extract_raw_laps(year, round_num, race_name):
    try:
        session = fastf1.get_session(year, round_num, "R")
        session.load(laps=True, telemetry=False, weather=False, messages=False)
        laps = session.laps.pick_drivers(DRIVERS).copy()
        if laps.empty:
            return pd.DataFrame()
        laps = laps[[c for c in RAW_COLS if c in laps.columns]].copy()
        laps["year"]      = year
        laps["round"]     = round_num
        laps["race_name"] = race_name
        return laps
    except Exception as e:
        print(f"    [{year} R{round_num}] skipped: {e}")
        return pd.DataFrame()


def run_laps_raw(out_path=None):
    out_path = Path(out_path or DATA_DIR / "laps_raw.csv")
    print("\n── Raw laps (FastF1, resumable) ──")
    if out_path.exists():
        existing = pd.read_csv(out_path)
        done = set(zip(existing["year"], existing["round"]))
        frames = [existing]
        print(f"  Loaded {len(existing)} existing rows covering {len(done)} rounds")
    else:
        done, frames = set(), []
        print("  No existing data — fetching from scratch")

    for year in ALPINE_YEARS:
        schedule = get_schedule(year)
        print(f"  {year} — {len(schedule)} rounds")
        for rnd, name in schedule:
            if (year, rnd) in done:
                print(f"    R{rnd} {name[:30]} — already saved")
                continue
            print(f"    R{rnd} {name[:30]}...", end=" ")
            df = extract_raw_laps(year, rnd, name)
            if not df.empty:
                frames.append(df)
                done.add((year, rnd))
                print(f"{len(df)} laps")
                pd.concat(frames, ignore_index=True).to_csv(out_path, index=False)
            else:
                print("no data")

    result = pd.concat(frames, ignore_index=True)
    print(f"  Saved {out_path} — {len(result)} laps across {result.groupby(['year','round']).ngroups} rounds")
    return result


# ── main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-ff1", action="store_true", help="Jolpica only")
    args = parser.parse_args()

    run_career()

    if not args.skip_ff1:
        run_qualifying()
        run_laps_raw()

    print("\nAll done.")
