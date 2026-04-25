# Gasly vs Ocon — Alpine Era Analysis

A structured head-to-head performance comparison of Pierre Gasly and Esteban Ocon during their time as Alpine teammates (2023–2024).

---

## What this project analyses

- **Career context** — full F1 career stats for both drivers to frame the Alpine stint
- **200th race milestone** — projection of when they reach 200 F1 starts
- **Alpine era summary** — points, DNF rates, H2H race and qualifying wins across four sub-periods: 2023 H1, 2023 H2, 2024 pre-Monaco, 2024 post-Monaco. The Monaco split reflects the 2024 collision between the two drivers which marked a clear inflection in their relationship
- **Race pace** — median clean lap time comparison by compound (HARD and MEDIUM) and season, with sample size and stint length transparency
- **Tyre degradation** — matched stint methodology with fuel load correction; methodology documented but excluded from conclusions due to fuel effect uncertainty

## Key findings

- **Qualifying**: Gasly held a consistent edge overall, with the gap widening in his favour across 2024
- **Race pace (HARD)**: Gasly faster in 2023 (matched stint lengths — credible); 2024 result unreliable as Ocon ran materially longer stints
- **Race pace (MEDIUM)**: Ocon faster in both seasons; stronger in 2024 despite running longer stints — the most robust pace finding
- **Points**: Ocon ahead on raw points in 2023; picture more mixed in 2024 after the Monaco incident

---

## Files

| File | Purpose |
|------|---------|
| `fetch_data.py` | Fetches all raw data from Jolpica (career results, sprint points, qualifying) and FastF1 (raw laps). Run this first. |
| `utils.py` | Shared constants, data cleaning helpers, calculation functions, formatting and plotting utilities. |
| `analysis.ipynb` | Main analysis notebook. Reads from CSVs only — no internet or FastF1 calls. |
| `data/` | CSV outputs from `fetch_data.py` — not committed to version control. |

---

## Setup & usage

### 1. Install dependencies

```bash
pip install fastf1 pandas numpy matplotlib requests
```

### 2. Fetch data

```bash
python3 fetch_data.py          # full fetch (career + qualifying + raw laps)
python3 fetch_data.py --skip-ff1   # career results only (fast, no FastF1)
```

Raw lap fetching is resumable — if interrupted, re-run and it picks up where it left off.

### 3. Run the notebook

Open `analysis.ipynb` in Jupyter and run all cells.

---

## Data sources

- **[Jolpica API](https://api.jolpi.ca/)** (Ergast-compatible) — career race results and sprint results
- **[FastF1](https://docs.fastf1.dev/)** — qualifying session results and lap-level race data

**Note:** Points totals include sprint race points (sourced separately via Jolpica). All other metrics — wins, podiums, DNF % — reflect full race results only.
