from pathlib import Path
import pandas as pd
import numpy as np


# ── 1. constants ──────────────────────────────────────────────────────────────

DATA_DIR    = Path("data")

DRIVERS      = ["GAS", "OCO"]
COLORS       = {"GAS": "#0090FF", "OCO": "#FF6B35"}
LABELS       = {"GAS": "Gasly",   "OCO": "Ocon"}
ALPINE_YEARS = [2023, 2024]

GENUINE_DNF = {"Mechanical DNF", "Incident DNF", "Retired (cause unknown)", "Other DNF"}
STREET_CIRCUITS = {
    "Saudi Arabian Grand Prix", "Azerbaijan Grand Prix", "Monaco Grand Prix",
    "Singapore Grand Prix", "Las Vegas Grand Prix",
}
MECHANICAL = {
    "engine", "gearbox", "hydraulics", "brakes", "electrical", "suspension",
    "transmission", "clutch", "oil pressure", "water leak", "fuel system",
    "turbo", "power unit", "ers", "mgu-k", "mgu-h", "exhaust", "wheel",
    "tyre", "puncture", "driveshaft", "overheating", "oil leak", "rear wing",
}
INCIDENT = {"accident", "collision", "spun off", "collision damage", "damage", "debris", "withdrew"}

S2023_SPLIT_ROUND = 11   # H1: rounds 1-11, H2: rounds 12-22
S2024_SPLIT_ROUND = 8    # pre-Monaco: rounds 1-8, post-Monaco: rounds 9-24
PERIODS = ["2023 H1", "2023 H2", "2024 pre-Monaco", "2024 post-Monaco"]

INT_METRICS = [
    "Races", "Points", "H2H race wins", "H2H quali wins",
    "DNF %", "Median grid", "Median pos gained",
]


# ── 2. data cleaning helpers ──────────────────────────────────────────────────

def categorise(status):
    """Classify a raw race retirement status string into a standard category.

    Input:
        status — raw status string from the results CSV (e.g. "Engine", "Collision", "+1 Lap").

    Output:
        One of: "Finished", "Classified", "Mechanical DNF", "Incident DNF",
                "Retired (cause unknown)", "Other DNF".
    """
    s = str(status).lower()
    if s == "finished":                    return "Finished"
    if s.startswith("+") or s == "lapped": return "Classified"
    if any(m in s for m in MECHANICAL):    return "Mechanical DNF"
    if any(i in s for i in INCIDENT):      return "Incident DNF"
    if "retired" in s or "did not" in s:   return "Retired (cause unknown)"
    return "Other DNF"


def _period_label(year, rnd):
    if year == 2023:
        return "2023 H1" if rnd <= S2023_SPLIT_ROUND else "2023 H2"
    return "2024 pre-Monaco" if rnd <= S2024_SPLIT_ROUND else "2024 post-Monaco"


def add_period_labels(df):
    """Add analysis period columns to a DataFrame in-place.

    Input:
        df — DataFrame with "year" and "round" columns.

    Output (columns added in-place):
        period       — human-readable period label, one of PERIODS.
        is_2023_h2   — 1 if the row belongs to 2023 H2, else 0.
        is_2024_pre  — 1 if the row belongs to 2024 pre-Monaco, else 0.
        is_2024_post — 1 if the row belongs to 2024 post-Monaco, else 0.
    """
    df["period"]      = df.apply(lambda r: _period_label(r["year"], r["round"]), axis=1)
    df["is_2023_h2"]  = (df["period"] == "2023 H2").astype(int)
    df["is_2024_pre"] = (df["period"] == "2024 pre-Monaco").astype(int)
    df["is_2024_post"]= (df["period"] == "2024 post-Monaco").astype(int)


# ── 3. calculations ───────────────────────────────────────────────────────────

def period_split(df, year, split_round):
    """Split a DataFrame into two halves of a season at a given round.

    Input:
        df          — DataFrame with "year" and "round" columns.
        year        — the season to split (e.g. 2023).
        split_round — last round of the first half (inclusive).

    Output:
        (pre, post) — two DataFrames; pre contains rounds <= split_round,
                      post contains rounds > split_round, both filtered to the given year.
    """
    return (
        df[(df["year"] == year) & (df["round"] <= split_round)],
        df[(df["year"] == year) & (df["round"] >  split_round)],
    )


def make_summary(alpine_s, quali_s, labels):
    """Build the Alpine-era performance summary table.

    Input:
        alpine_s — list of alpine race result DataFrames, one per period.
        quali_s  — list of qualifying H2H DataFrames, one per period.
        labels   — list of period label strings (e.g. PERIODS).

    Output:
        DataFrame with metrics as rows and (period, driver) as a MultiIndex column.
        Metrics: Races, Points, DNF %, Pts/race, H2H race wins, H2H quali wins,
                 Median grid, Median pos gained.
    """
    frames = []
    for label, alp, qua in zip(labels, alpine_s, quali_s):
        alp = alp.copy()
        aw = (alp.pivot_table(index="round", columns="driver", values="finish_pos", aggfunc="first")
              .dropna(subset=["GAS", "OCO"]).reset_index())

        pts = (
            alp.groupby("driver").agg(races=("round", "count"), pts=("total_points", "sum"))
            .assign(
                pts_per_race=lambda d: (d["pts"] / d["races"]).round(2),
                dnf_rate=lambda d: (
                    alp[alp["status_cat"].isin(GENUINE_DNF)]
                    .groupby("driver").size()
                    .reindex(d.index, fill_value=0) / d["races"] * 100
                ).round(0),
            )
            .reset_index().melt(id_vars="driver", var_name="metric", value_name="value")
        )
        pts["metric"] = pts["metric"].replace(
            {"races": "Races", "pts": "Points", "pts_per_race": "Pts/race", "dnf_rate": "DNF %"}
        )

        pg = (
            alp[alp["grid"].gt(0) & alp["finish_pos"].notna()]
            .groupby("driver")["pos_gained"].median().round(0).reset_index()
            .rename(columns={"pos_gained": "value"}).assign(metric="Median pos gained")
        )

        grid = (
            alp[alp["grid"].gt(0)]
            .groupby("driver")["grid"].median().round(0).reset_index()
            .rename(columns={"grid": "value"}).assign(metric="Median grid")
        )

        aw["winner"] = aw.apply(lambda r: "GAS" if r["GAS"] < r["OCO"] else "OCO", axis=1)
        h2h_r = (aw.groupby("winner").size().reset_index(name="value")
                 .rename(columns={"winner": "driver"})
                 .assign(metric="H2H race wins")[["metric", "driver", "value"]])

        h2h_q = (qua.groupby("faster").size().reset_index(name="value")
                 .query("faster in ['GAS','OCO']")
                 .rename(columns={"faster": "driver"})
                 .assign(metric="H2H quali wins")[["metric", "driver", "value"]])

        part = pd.concat([pts, grid, pg, h2h_r, h2h_q], ignore_index=True)
        part["period"] = label
        frames.append(part)

    long = pd.concat(frames, ignore_index=True)
    tbl = (
        long.pivot_table(index="metric", columns=["period", "driver"], values="value")
        .reindex(columns=pd.MultiIndex.from_product([labels, ["GAS", "OCO"]]))
        .reindex([
            "Races", "Points", "DNF %", "Pts/race",
            "H2H race wins", "H2H quali wins",
            "Median grid", "Median pos gained",
        ])
    )
    COUNT = ["Races", "H2H race wins", "H2H quali wins"]
    tbl.loc[tbl.index.isin(COUNT)] = tbl.loc[tbl.index.isin(COUNT)].fillna(0)
    return tbl


def get_season_calendar(year):
    """Return the race calendar for a given F1 season.

    Input:
        year — the season year (e.g. 2025).

    Output:
        List of (round_number, event_name) tuples, e.g. [(1, "Bahrain Grand Prix"), ...].
        Loaded from the local FastF1 cache — does not require an internet connection
        if the schedule has been cached previously.
    """
    import fastf1
    fastf1.Cache.enable_cache(str(DATA_DIR / "ff1_cache"))
    sched = fastf1.get_event_schedule(year, include_testing=False)
    return [(int(r.RoundNumber), r.EventName) for _, r in sched.iterrows()]


# ── 4. formatting helpers ─────────────────────────────────────────────────────

def color_driver(val):
    """Return CSS colour for a value — blue if positive (Gasly ahead), orange if negative (Ocon ahead)."""
    if pd.isna(val) or val == 0: return ""
    return "color: #0090FF" if val > 0 else "color: #FF6B35"


def display_summary(tbl):
    """Render the summary table with appropriate number formatting.

    Input:
        tbl — DataFrame produced by make_summary().

    Output:
        Styled IPython display: integer metrics formatted as whole numbers,
        float metrics formatted to 2 decimal places, NaN shown as blank.
    """
    from IPython.display import display
    int_rows   = [m for m in INT_METRICS if m in tbl.index]
    float_rows = [m for m in tbl.index if m not in INT_METRICS]
    display(
        tbl.style
        .format("{:.0f}", subset=pd.IndexSlice[int_rows,   :], na_rep="")
        .format("{:.2f}", subset=pd.IndexSlice[float_rows, :], na_rep="")
    )


# ── 5. plotting ───────────────────────────────────────────────────────────────

def poly_curve(x, y, max_pos):
    coef = np.polyfit(x, y, 2)
    xs = np.arange(1, max_pos + 1)
    return xs, np.polyval(coef, xs)


def plot_deg_curves(df, col, ylabel, title, pace_compounds, savepath=None):
    import matplotlib.pyplot as plt
    seasons = sorted(df['season'].unique())
    fig, axes = plt.subplots(len(pace_compounds), len(seasons), figsize=(12, 4 * len(pace_compounds)), sharey='row')
    for row, compound in enumerate(pace_compounds):
        sub = df[df['compound'] == compound]
        for c, season in enumerate(seasons):
            ax = axes[row, c]
            p = sub[sub['season'] == season]
            max_pos = int(p['stint_lap'].quantile(0.75)) if len(p) else 10
            for driver in DRIVERS:
                d = p[p['driver'] == driver]
                ax.scatter(d['stint_lap'], d[col], color=COLORS[driver], alpha=0.08, s=4)
                if len(d) >= 6:
                    xs, ys = poly_curve(d['stint_lap'].values, d[col].values, max_pos)
                    ax.plot(xs, ys, color=COLORS[driver], label=LABELS[driver], linewidth=2)
            ax.axhline(0, color='grey', linewidth=0.5, linestyle='--')
            ax.set_title(f'{compound} — {season}', fontsize=9)
            ax.set_xlabel('Lap in stint')
            ax.set_xlim(1, max_pos)
            step = max(1, max_pos // 8)
            ax.set_xticks(range(1, max_pos + 1, step))
            ax.set_ylim(-3, 3)
            if c == 0:
                ax.set_ylabel(ylabel)
                ax.legend(fontsize=8)
    plt.suptitle(title, fontsize=10, y=1.01)
    plt.tight_layout()
    if savepath:
        plt.savefig(savepath, dpi=150, bbox_inches='tight')
    plt.show()
