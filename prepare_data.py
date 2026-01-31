# dwts_preprocess_eda.py
# -*- coding: utf-8 -*-
"""
DWTS (2026 MCM Problem C) preprocessing + EDA
Outputs:
  - cleaned wide table
  - long judge-score table
  - weekly aggregates
  - contestant summary
  - season summary
  - EDA figures (beautiful, publication-ready)

Dependencies:
  pip install pandas numpy matplotlib seaborn
(Optional for parquet)
  pip install pyarrow
"""

from __future__ import annotations
import re
import os
from pathlib import Path
import argparse
import numpy as np
import pandas as pd

import matplotlib.pyplot as plt
import seaborn as sns


# -------------------------
# Config: plotting
# -------------------------
def set_plot_style():
    sns.set_theme(
        style="whitegrid",
        context="talk",
        font="DejaVu Sans",
    )
    plt.rcParams.update({
        "figure.dpi": 140,
        "savefig.dpi": 300,
        "axes.titleweight": "bold",
        "axes.labelweight": "bold",
        "axes.spines.top": False,
        "axes.spines.right": False,
    })


# -------------------------
# Helpers
# -------------------------
WEEK_JUDGE_RE = re.compile(r"^week(\d+)_judge(\d+)_score$")


def find_score_cols(columns):
    score_cols = [c for c in columns if WEEK_JUDGE_RE.match(c)]
    # sort by week then judge for nice ordering
    def key(c):
        w, j = WEEK_JUDGE_RE.match(c).groups()
        return (int(w), int(j))
    return sorted(score_cols, key=key)


def parse_elim_week_from_results(s: str):
    """results examples:
       'Eliminated Week 3', '1st Place', '2nd Place', 'Withdrew', ...
    """
    if pd.isna(s):
        return np.nan
    m = re.search(r"Eliminated\s+Week\s*(\d+)", str(s))
    return int(m.group(1)) if m else np.nan


def safe_to_parquet(df: pd.DataFrame, path: Path):
    """Write parquet if possible; otherwise fallback to csv."""
    try:
        df.to_parquet(path, index=False)
        return True
    except Exception:
        df.to_csv(path.with_suffix(".csv"), index=False, encoding="utf-8-sig")
        return False


# -------------------------
# Core preprocessing
# -------------------------
def load_raw(input_csv: Path) -> pd.DataFrame:
    df = pd.read_csv(input_csv)
    # Rename column to avoid slash in name
    if "celebrity_homecountry/region" in df.columns:
        df = df.rename(columns={"celebrity_homecountry/region": "celebrity_homecountry_region"})

    # Basic cleanup for strings
    str_cols = [
        "celebrity_name", "ballroom_partner", "celebrity_industry",
        "celebrity_homestate", "celebrity_homecountry_region", "results"
    ]
    for c in str_cols:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip().replace({"nan": np.nan})

    # Enforce numeric types
    if "season" in df.columns:
        df["season"] = pd.to_numeric(df["season"], errors="coerce").astype("Int64")
    if "placement" in df.columns:
        df["placement"] = pd.to_numeric(df["placement"], errors="coerce").astype("Int64")
    if "celebrity_age_during_season" in df.columns:
        df["celebrity_age_during_season"] = pd.to_numeric(df["celebrity_age_during_season"], errors="coerce")

    return df


def preprocess(df_raw: pd.DataFrame):
    df = df_raw.copy()
    score_cols = find_score_cols(df.columns)

    # Convert score cols to numeric
    df[score_cols] = df[score_cols].apply(pd.to_numeric, errors="coerce")

    # IMPORTANT:
    # In this dataset, "0 score" indicates eliminated (or not competing) after that week.
    # We treat 0 as missing score (not a real judge score).
    df_scores = df.copy()
    df_scores[score_cols] = df_scores[score_cols].mask(df_scores[score_cols] == 0, np.nan)

    # Build long judge score table
    id_cols = [
        "celebrity_name", "ballroom_partner", "celebrity_industry",
        "celebrity_homestate", "celebrity_homecountry_region",
        "celebrity_age_during_season", "season", "results", "placement",
    ]
    # keep only those that exist (robust)
    id_cols = [c for c in id_cols if c in df_scores.columns]

    long = df_scores.melt(
        id_vars=id_cols,
        value_vars=score_cols,
        var_name="week_judge",
        value_name="judge_score",
    )
    long[["week", "judge"]] = long["week_judge"].str.extract(r"^week(\d+)_judge(\d+)_score$").astype(int)
    long = long.drop(columns=["week_judge"])

    # Weekly aggregate per contestant
    weekly = (
        long.groupby(["season", "celebrity_name", "ballroom_partner", "week"], as_index=False)
            .agg(
                judge_total=("judge_score", "sum"),
                n_judges=("judge_score", "count"),
                judge_mean=("judge_score", "mean"),
            )
    )
    weekly["competed"] = weekly["n_judges"] > 0

    # Last competed week (use competed=True)
    last_week = (
        weekly[weekly["competed"]]
        .groupby(["season", "celebrity_name"], as_index=False)["week"]
        .max()
        .rename(columns={"week": "last_competed_week"})
    )

    # Elimination week from results, then fallback to last_competed_week for finalists/withdrew
    meta = df_scores[id_cols].copy()
    meta["elim_week_from_results"] = meta["results"].apply(parse_elim_week_from_results)

    meta = meta.merge(last_week, on=["season", "celebrity_name"], how="left")
    meta["elim_week"] = meta["elim_week_from_results"].fillna(meta["last_competed_week"])

    # Season length and contestant count
    season_summary = (
        meta.groupby("season", as_index=False)
            .agg(
                n_contestants=("celebrity_name", "count"),
                season_length=("last_competed_week", "max"),
                n_industries=("celebrity_industry", pd.Series.nunique),
            )
            .sort_values("season")
    )

    # Contestant summary (features for modeling)
    contestant_summary = (
        weekly[weekly["competed"]]
        .groupby(["season", "celebrity_name", "ballroom_partner"], as_index=False)
        .agg(
            avg_judge_total=("judge_total", "mean"),
            std_judge_total=("judge_total", "std"),
            avg_judge_mean=("judge_mean", "mean"),
            weeks_competed=("week", "nunique"),
            best_week_total=("judge_total", "max"),
        )
    )
    contestant_summary = contestant_summary.merge(
        meta[["season", "celebrity_name", "placement", "results", "elim_week",
              "celebrity_industry", "celebrity_age_during_season",
              "celebrity_homestate", "celebrity_homecountry_region"]],
        on=["season", "celebrity_name"],
        how="left",
    )

    # Extra weekly features (useful for vote estimation / later models)
    # - within-week judge rank (1 = best) per season-week
    weekly_feat = weekly.copy()
    weekly_feat["judge_rank_in_week"] = weekly_feat.groupby(["season", "week"])["judge_total"] \
        .rank(method="min", ascending=False)
    weekly_feat["judge_percent_in_week"] = weekly_feat["judge_total"] / weekly_feat.groupby(["season", "week"])["judge_total"] \
        .transform("sum")
    # normalized by max possible (10 points per judge)
    weekly_feat["judge_total_norm"] = weekly_feat["judge_total"] / (10.0 * weekly_feat["n_judges"].replace(0, np.nan))

    # missingness table (week x judge)
    miss = pd.DataFrame({"col": score_cols})
    miss[["week", "judge"]] = miss["col"].str.extract(r"^week(\d+)_judge(\d+)_score$").astype(int)
    miss["missing_rate"] = df_scores[score_cols].isna().mean().values
    miss_pivot = miss.pivot(index="week", columns="judge", values="missing_rate").sort_index()

    return {
        "df_raw": df_raw,
        "df_clean_wide": df_scores,
        "score_cols": score_cols,
        "long": long,
        "weekly": weekly_feat,
        "contestant_summary": contestant_summary,
        "season_summary": season_summary,
        "missing_pivot": miss_pivot,
    }


# -------------------------
# EDA Plots
# -------------------------
def savefig(fig, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def plot_season_contestants(season_summary: pd.DataFrame, outdir: Path):
    fig, ax = plt.subplots(figsize=(12, 5))
    sns.lineplot(data=season_summary, x="season", y="n_contestants", marker="o", ax=ax)
    ax.set_title("Number of Contestants per Season")
    ax.set_xlabel("Season")
    ax.set_ylabel("Contestants")
    savefig(fig, outdir / "figures" / "season_n_contestants.png")


def plot_season_length(season_summary: pd.DataFrame, outdir: Path):
    fig, ax = plt.subplots(figsize=(12, 5))
    sns.lineplot(data=season_summary, x="season", y="season_length", marker="o", ax=ax)
    ax.set_title("Season Length (Max Week with Competition)")
    ax.set_xlabel("Season")
    ax.set_ylabel("Weeks")
    savefig(fig, outdir / "figures" / "season_length.png")


def plot_age_distribution(df_clean_wide: pd.DataFrame, outdir: Path):
    fig, ax = plt.subplots(figsize=(10, 5))
    sns.histplot(df_clean_wide["celebrity_age_during_season"].dropna(), bins=20, kde=True, ax=ax)
    ax.set_title("Celebrity Age Distribution")
    ax.set_xlabel("Age during Season")
    ax.set_ylabel("Count")
    savefig(fig, outdir / "figures" / "age_distribution.png")


def plot_industry_counts(df_clean_wide: pd.DataFrame, outdir: Path, topk: int = 15):
    ind = df_clean_wide["celebrity_industry"].value_counts().head(topk).reset_index()
    ind.columns = ["industry", "count"]

    fig, ax = plt.subplots(figsize=(12, 6))
    sns.barplot(data=ind, y="industry", x="count", ax=ax)
    ax.set_title(f"Top {topk} Celebrity Industries (Count)")
    ax.set_xlabel("Count")
    ax.set_ylabel("")
    savefig(fig, outdir / "figures" / "industry_topk.png")


def plot_missingness_heatmap(missing_pivot: pd.DataFrame, outdir: Path):
    fig, ax = plt.subplots(figsize=(8, 6))
    sns.heatmap(
        missing_pivot,
        annot=True,
        fmt=".2f",
        cbar_kws={"label": "Missing Rate"},
        ax=ax
    )
    ax.set_title("Missing Rate of Judge Scores (Week x Judge)")
    ax.set_xlabel("Judge")
    ax.set_ylabel("Week")
    savefig(fig, outdir / "figures" / "missingness_week_judge.png")


def plot_avg_judge_total_by_week(weekly: pd.DataFrame, outdir: Path):
    # average across all seasons, only competed weeks
    tmp = weekly[weekly["competed"]].groupby("week", as_index=False)["judge_total"].mean()

    fig, ax = plt.subplots(figsize=(10, 5))
    sns.lineplot(data=tmp, x="week", y="judge_total", marker="o", ax=ax)
    ax.set_title("Average Total Judge Score by Week (Across All Seasons)")
    ax.set_xlabel("Week")
    ax.set_ylabel("Avg Total Judge Score")
    savefig(fig, outdir / "figures" / "avg_judge_total_by_week.png")


def plot_season_week_heatmap(weekly: pd.DataFrame, outdir: Path):
    # season x week matrix of avg judge_total
    tmp = (weekly[weekly["competed"]]
           .groupby(["season", "week"], as_index=False)["judge_total"].mean())
    mat = tmp.pivot(index="season", columns="week", values="judge_total").sort_index()

    fig, ax = plt.subplots(figsize=(12, 9))
    sns.heatmap(mat, cmap="viridis", cbar_kws={"label": "Avg Total Judge Score"}, ax=ax)
    ax.set_title("Avg Total Judge Score Heatmap (Season x Week)")
    ax.set_xlabel("Week")
    ax.set_ylabel("Season")
    savefig(fig, outdir / "figures" / "season_week_judge_total_heatmap.png")


def plot_placement_vs_score(contestant_summary: pd.DataFrame, outdir: Path):
    # placement: 1 is best; score: higher is better
    tmp = contestant_summary.dropna(subset=["placement", "avg_judge_total"]).copy()
    tmp["placement"] = tmp["placement"].astype(int)

    fig, ax = plt.subplots(figsize=(10, 6))
    sns.regplot(data=tmp, x="avg_judge_total", y="placement", scatter_kws={"alpha": 0.6}, ax=ax)
    ax.set_title("Placement vs. Avg Judge Total (Across Seasons)")
    ax.set_xlabel("Avg Weekly Total Judge Score")
    ax.set_ylabel("Final Placement (1 = best)")
    ax.invert_yaxis()  # nicer: best at top
    savefig(fig, outdir / "figures" / "placement_vs_avg_score.png")


def plot_industry_score_box(contestant_summary: pd.DataFrame, outdir: Path, topk: int = 10):
    # Focus on industries with enough samples
    vc = contestant_summary["celebrity_industry"].value_counts()
    keep = vc.head(topk).index
    tmp = contestant_summary[contestant_summary["celebrity_industry"].isin(keep)].copy()

    fig, ax = plt.subplots(figsize=(14, 7))
    sns.boxplot(data=tmp, x="celebrity_industry", y="avg_judge_total", ax=ax)
    ax.set_title(f"Avg Weekly Judge Total by Industry (Top {topk})")
    ax.set_xlabel("")
    ax.set_ylabel("Avg Weekly Total Judge Score")
    ax.tick_params(axis="x", rotation=25)
    savefig(fig, outdir / "figures" / "industry_box_avg_score.png")


def run_eda(artifacts: dict, outdir: Path):
    df_clean = artifacts["df_clean_wide"]
    season_summary = artifacts["season_summary"]
    weekly = artifacts["weekly"]
    contestant_summary = artifacts["contestant_summary"]
    missing_pivot = artifacts["missing_pivot"]

    plot_season_contestants(season_summary, outdir)
    plot_season_length(season_summary, outdir)
    plot_age_distribution(df_clean, outdir)
    plot_industry_counts(df_clean, outdir, topk=15)
    plot_missingness_heatmap(missing_pivot, outdir)
    plot_avg_judge_total_by_week(weekly, outdir)
    plot_season_week_heatmap(weekly, outdir)
    plot_placement_vs_score(contestant_summary, outdir)
    plot_industry_score_box(contestant_summary, outdir, topk=10)


# -------------------------
# Save artifacts
# -------------------------
def save_outputs(artifacts: dict, outdir: Path):
    outdir = Path(outdir)
    (outdir / "data").mkdir(parents=True, exist_ok=True)

    df_clean = artifacts["df_clean_wide"]
    long = artifacts["long"]
    weekly = artifacts["weekly"]
    contestant_summary = artifacts["contestant_summary"]
    season_summary = artifacts["season_summary"]

    # Wide cleaned
    df_clean.to_csv(outdir / "data" / "dwts_clean_wide.csv", index=False, encoding="utf-8-sig")

    # Long + weekly + summaries (parquet preferred)
    safe_to_parquet(long, outdir / "data" / "dwts_long_scores.parquet")
    safe_to_parquet(weekly, outdir / "data" / "dwts_weekly.parquet")
    contestant_summary.to_csv(outdir / "data" / "dwts_contestant_summary.csv", index=False, encoding="utf-8-sig")
    season_summary.to_csv(outdir / "data" / "dwts_season_summary.csv", index=False, encoding="utf-8-sig")


# -------------------------
# Main
# -------------------------
def main():
    parser = argparse.ArgumentParser(description="DWTS preprocessing + EDA")
    parser.add_argument(
        "--input",
        type=str,
        default="/math/data/2026_MCM_Problem_C_Data.csv",
        help="Path to 2026_MCM_Problem_C_Data.csv",
    )
    parser.add_argument(
        "--outdir",
        type=str,
        default="dwts_outputs",
        help="Output directory for cleaned data & figures",
    )
    args = parser.parse_args()

    set_plot_style()

    input_csv = Path(args.input)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    df_raw = load_raw(input_csv)
    artifacts = preprocess(df_raw)

    save_outputs(artifacts, outdir)
    run_eda(artifacts, outdir)

    print("Done.")
    print(f"- Cleaned data saved to: {outdir / 'data'}")
    print(f"- Figures saved to:      {outdir / 'figures'}")


if __name__ == "__main__":
    main()
