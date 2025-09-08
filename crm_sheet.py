"""Lightweight CRM Sheet management for VC sourcing agent.

This module creates and maintains a Google Sheet with three tabs acting as
an extremely light CRM:

* Inbox  – every newly discovered row
* Review – borderline scores that warrant human evaluation
* Leads  – high scoring rows considered qualified

Each sheet uses the same audit-friendly columns:

    Company, URL, Source, Date, Country Guess, Signals, Score, Confidence,
    Reason Codes, Owner, Status, Next Step, Due, Evidence, Cluster ID, Run ID

Routing logic writes high-score rows to the **Leads** sheet, borderline
rows to **Review**, and drops the rest.  Only the freshly appended rows are
stored in ``data/new_rows.csv`` so downstream systems can track what changed
in this run.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Optional
import os

import pandas as pd

try:
    import gspread
    from gspread_dataframe import set_with_dataframe
except Exception:  # pragma: no cover - import guard for environments w/o creds
    gspread = None
    set_with_dataframe = None

# Ordered list of column headers used across all tabs.
HEADERS: List[str] = [
    "Company",
    "URL",
    "Source",
    "Date",
    "Country Guess",
    "Signals",
    "Score",
    "Confidence",
    "Reason Codes",
    "Owner",
    "Status",
    "Next Step",
    "Due",
    "Evidence",
    "Cluster ID",
    "Run ID",
]


@dataclass
class RoutingConfig:
    """Thresholds controlling which sheet a row is routed to."""

    high_threshold: float = 0.8
    review_threshold: float = 0.5


def _ensure_headers(ws) -> None:
    """Make sure a worksheet contains the CRM headers."""
    existing = ws.row_values(1)
    if existing[: len(HEADERS)] != HEADERS:
        ws.clear()
        ws.append_row(HEADERS)


def setup_spreadsheet(spreadsheet) -> None:
    """Ensure required tabs exist with proper headers."""
    for title in ("Inbox", "Review", "Leads"):
        try:
            ws = spreadsheet.worksheet(title)
        except Exception:  # WorksheetNotFound, but keep import-agnostic
            ws = spreadsheet.add_worksheet(title=title, rows=1000, cols=len(HEADERS))
        _ensure_headers(ws)


def route_rows(df: pd.DataFrame, config: RoutingConfig) -> pd.Series:
    """Return target sheet for each row based on score."""
    def _route(score: float) -> Optional[str]:
        if score >= config.high_threshold:
            return "Leads"
        if score >= config.review_threshold:
            return "Review"
        return None

    return df["Score"].apply(_route)


def append_rows(spreadsheet, rows: Iterable[dict], run_id: str, config: RoutingConfig | None = None) -> pd.DataFrame:
    """Append rows to the CRM sheets and persist new_rows.csv.

    Parameters
    ----------
    spreadsheet : gspread.Spreadsheet
        Target spreadsheet obtained from ``gspread``.
    rows : Iterable[dict]
        Collection of rows to append.  Each row should provide at least the
        headers defined in ``HEADERS`` minus the system-managed ones.
    run_id : str
        Identifier written to the ``Run ID`` column for traceability.
    config : RoutingConfig, optional
        Threshold configuration.  Defaults to ``RoutingConfig()``.

    Returns
    -------
    pandas.DataFrame
        DataFrame of rows that were appended (Inbox rows).
    """

    if config is None:
        config = RoutingConfig()

    df = pd.DataFrame(list(rows))

    # Ensure all required columns exist.
    for col in HEADERS:
        if col not in df.columns:
            df[col] = ""

    df = df[HEADERS]
    df["Run ID"] = run_id

    # Determine routing and status values.
    destinations = route_rows(df, config)

    inbox_df = df.copy()
    inbox_df["Status"] = "Inbox"

    leads_df = df[destinations == "Leads"].copy()
    leads_df["Status"] = "Qualified"

    review_df = df[destinations == "Review"].copy()
    review_df["Status"] = "Review"

    # Perform spreadsheet writes when gspread is available.
    if gspread is not None and set_with_dataframe is not None:
        setup_spreadsheet(spreadsheet)
        inbox_ws = spreadsheet.worksheet("Inbox")
        set_with_dataframe(
            inbox_ws, inbox_df, row=inbox_ws.row_count + 1, include_column_header=False
        )
        if not leads_df.empty:
            leads_ws = spreadsheet.worksheet("Leads")
            set_with_dataframe(
                leads_ws, leads_df, row=leads_ws.row_count + 1, include_column_header=False
            )
        if not review_df.empty:
            review_ws = spreadsheet.worksheet("Review")
            set_with_dataframe(
                review_ws, review_df, row=review_ws.row_count + 1, include_column_header=False
            )

    # Persist newly appended rows for downstream consumers.
    os.makedirs("data", exist_ok=True)
    inbox_df.to_csv("data/new_rows.csv", index=False)
    return inbox_df


if __name__ == "__main__":  # pragma: no cover - illustrative usage
    # Example usage reading candidate rows from a CSV file.
    import argparse

    parser = argparse.ArgumentParser(description="Append rows to CRM sheet")
    parser.add_argument("csv", help="CSV file containing new rows")
    parser.add_argument("run_id", help="Identifier for this run")
    parser.add_argument("spreadsheet_id", help="Target Google Spreadsheet ID")
    parser.add_argument("--creds", help="Path to gspread service account JSON")
    parser.add_argument(
        "--high", type=float, default=0.8, help="Score threshold for Leads routing"
    )
    parser.add_argument(
        "--review", type=float, default=0.5, help="Score threshold for Review routing"
    )
    args = parser.parse_args()

    df_input = pd.read_csv(args.csv)

    gc = gspread.service_account(filename=args.creds) if args.creds else gspread.service_account()
    ss = gc.open_by_key(args.spreadsheet_id)

    append_rows(ss, df_input.to_dict("records"), args.run_id, RoutingConfig(args.high, args.review))
