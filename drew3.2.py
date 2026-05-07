#!/usr/bin/env python3
"""
NCUA 5300 Call Report Financial Dashboard Generator
====================================================
Prompts for a credit union name, searches NCUA bulk call-report data,
downloads the last 3 completed quarters of 5300 reports, sends them to
ChatGPT for analysis, and generates a self-contained HTML dashboard
with 10 key financial ratios and 3-quarter trend charts.

Requirements:
    pip install requests pandas openai plotly

Environment Variables:
    OPENAI_API_KEY   Your OpenAI API key (enables the AI analysis section)

Usage:
    python ncua_dashboard.py
"""

import io
import csv
import html as html_lib
import json
import os
import re
import sys
import webbrowser
import zipfile
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
import requests
from plotly.subplots import make_subplots

# ─────────────────────────────────────────────────────────────────────────────
# NCUA Data Source
# ─────────────────────────────────────────────────────────────────────────────

# Official quarterly bulk download URLs (try both with/without www)
NCUA_ZIP_URLS = [
    "https://ncua.gov/files/publications/analysis/call-report-data-{year}-{month:02d}.zip",
    "https://www.ncua.gov/files/publications/analysis/call-report-data-{year}-{month:02d}.zip",
]

# Local cache – avoids re-downloading large zips on repeated runs
CACHE_DIR = Path.home() / ".cache" / "ncua_5300"

# ─────────────────────────────────────────────────────────────────────────────
# NCUA 5300 Account Code Mappings
# Each key maps to a prioritised list of column-name variants so the script
# works across different NCUA format revisions.
# ─────────────────────────────────────────────────────────────────────────────

# All codes are normalised to UPPERCASE at read time (see _read_zip_file).
# Fields marked (FS220A) come from the supplemental file; the extractor merges both.
ACCT: dict[str, list[str]] = {
    # ── FS220.txt (main file) ────────────────────────────────────────────────
    "total_assets":        ["ACCT_010"],          # Total Assets
    "total_loans":         ["ACCT_025B"],          # Total Loans & Leases (net)
    "total_shares":        ["ACCT_018"],          # Total Shares & Deposits
    "members":             ["ACCT_083"],          # Number of current members (FS220)
    "delinquent":          ["ACCT_041B"],          # Delinquent 2+ months
    "opex_ytd":            ["ACCT_671"],          # Total Non-Interest Expense YTD
    "gross_chargeoffs_ytd":["ACCT_550"],          # Gross charge-offs YTD
    "recoveries_ytd":      ["ACCT_551"],          # Recoveries on charged-off loans YTD

    # ── FS220A.txt (supplemental file, merged in) ────────────────────────────
    "net_worth":           ["ACCT_997"],          # Total Net Worth  (FS220A)
    "net_income_ytd":      ["ACCT_661A"],         # Net Income (Loss) YTD  (FS220A)
    "interest_income_ytd": ["ACCT_110"],          # Interest on Loans YTD  (FS220A)
    "invest_income_ytd":   ["ACCT_120"],          # Investment Income YTD  (FS220A)
    "funding_costs_ytd":   ["ACCT_350"],          # Total Dividends+Interest Expense YTD (FS220A)
    "fee_income_ytd":          ["ACCT_131"],          # Fee Income YTD  (FS220A)
    "gain_on_assets_ytd":      ["ACCT_430"],          # Gain on sale of assets YTD (FS220A)
    "other_nonop_income_ytd":  ["ACCT_440"],          # Other non-operating income YTD (FS220A)
    "total_nonint_income_ytd": ["ACCT_117"],          # Total Non-Interest Income (FS220A) — preferred for efficiency ratio

    # ── FS220A.txt (liquidity cash components) ──────────────────────────────
    "invest_cash_730a":     ["ACCT_730A"],        # Cash component A  (FS220A)
    "invest_cash_730b":     ["ACCT_730B"],        # Cash component B  (FS220A)

    # ── FS220Q.txt (investment maturity schedule, merged in) ─────────────────
    "invest_short_term":    ["ACCT_NV0153"],      # Total investments maturing < 1 yr (FS220Q)
    "invest_1_3yr":         ["ACCT_NV0154"],      # Total investments maturing 1-3 yr (FS220Q)
    "invest_3_5yr":         ["ACCT_NV0155"],      # Total investments maturing 3-5 yr (FS220Q)
    "invest_5_10yr":        ["ACCT_NV0156"],      # Total investments maturing 5-10 yr (FS220Q)
    "invest_10yr_plus":     ["ACCT_NV0157"],      # Total investments maturing > 10 yr (FS220Q)

    # ── FS220P.txt (investment schedule, merged in) ──────────────────────────
    "invest_cash_deposits": ["ACCT_AS0009"],      # Total Cash & Other Deposits  (FS220P)
    "invest_securities":    ["ACCT_AS0013"],      # Total Investment Securities  (FS220P)
    "invest_other":         ["ACCT_AS0017"],      # Total Other Investments      (FS220P)

    # ── Loan rates (basis points stored by NCUA; ÷ 100 → %) ─────────────────
    # ACCT_560/561 were retired pre-1989; current fields are ACCT_521-524 in FS220 main file
    "rate_credit_card":      ["ACCT_521"],         # Credit card loan rate (FS220)
    "rate_other_unsecured":  ["ACCT_522"],         # All other unsecured loan rate (FS220)
    "rate_new_auto":         ["ACCT_523"],         # New vehicle loan rate (FS220)
    "rate_used_auto":        ["ACCT_524"],         # Used vehicle loan rate (FS220)
    "rate_leases":           ["ACCT_565"],         # Lease receivable rate (FS220A)
    "rate_pal":              ["ACCT_522A"],        # PAL loan rate (FS220H)
    "rate_student":          ["ACCT_595A"],        # Student loan rate (FS220H)
    "rate_other_secured":    ["ACCT_595B"],        # Other secured non-RE rate (FS220L)
    "rate_re_1st_lien":      ["ACCT_563A"],        # RE 1st lien rate (FS220L)
    "rate_re_junior_lien":   ["ACCT_562A"],        # RE junior lien rate (FS220L)
    "rate_re_other":         ["ACCT_562B"],        # Other RE rate (FS220L)
    "rate_commercial_re":    ["ACCT_525"],         # Commercial RE rate (FS220L)
    "rate_commercial_nonre": ["ACCT_526"],         # Commercial non-RE rate (FS220L)

    # ── Charge-offs YTD by loan category ────────────────────────────────────
    "co_credit_card":        ["ACCT_680"],         # CC charge-offs YTD (FS220B)
    "rec_credit_card":       ["ACCT_681"],         # CC recoveries YTD (FS220B)
    "co_pal":                ["ACCT_136"],         # PAL charge-offs YTD (FS220H)
    "rec_pal":               ["ACCT_137"],         # PAL recoveries YTD (FS220H)
    "co_student":            ["ACCT_550T"],        # Student loan charge-offs YTD (FS220H)
    "rec_student":           ["ACCT_551T"],        # Student loan recoveries YTD (FS220H)
    "co_other_unsecured":    ["ACCT_CH0007"],      # Other unsecured charge-offs (FS220P)
    "rec_other_unsecured":   ["ACCT_CH0008"],      # Other unsecured recoveries (FS220P)
    "co_new_vehicle":        ["ACCT_550C1"],       # New vehicle charge-offs YTD (FS220I)
    "rec_new_vehicle":       ["ACCT_551C1"],       # New vehicle recoveries YTD (FS220I)
    "co_used_vehicle":       ["ACCT_550C2"],       # Used vehicle charge-offs YTD (FS220I)
    "rec_used_vehicle":      ["ACCT_551C2"],       # Used vehicle recoveries YTD (FS220I)
    "co_leases":             ["ACCT_550D"],        # Lease charge-offs YTD (FS220C)
    "rec_leases":            ["ACCT_551D"],        # Lease recoveries YTD (FS220C)
    "co_other_secured":      ["ACCT_CH0015"],      # Other secured non-RE charge-offs (FS220P)
    "rec_other_secured":     ["ACCT_CH0016"],      # Other secured non-RE recoveries (FS220P)
    "co_re_1st_lien":        ["ACCT_CH0017"],      # RE 1st lien charge-offs (FS220P)
    "rec_re_1st_lien":       ["ACCT_CH0018"],      # RE 1st lien recoveries (FS220P)
    "co_re_junior_lien":     ["ACCT_CH0019"],      # RE junior lien charge-offs (FS220P)
    "rec_re_junior_lien":    ["ACCT_CH0020"],      # RE junior lien recoveries (FS220P)
    "co_re_other":           ["ACCT_CH0021"],      # Other RE charge-offs (FS220P)
    "rec_re_other":          ["ACCT_CH0022"],      # Other RE recoveries (FS220P)
    # Commercial RE sub-categories (summed in extract_loan_losses)
    "co_comm_re_constr":     ["ACCT_CH0023"],      # Construction & development
    "rec_comm_re_constr":    ["ACCT_CH0024"],
    "co_comm_re_farm":       ["ACCT_CH0025"],      # Farmland
    "rec_comm_re_farm":      ["ACCT_CH0026"],
    "co_comm_re_multi":      ["ACCT_CH0027"],      # Multifamily
    "rec_comm_re_multi":     ["ACCT_CH0028"],
    "co_comm_re_owner":      ["ACCT_CH0029"],      # Owner-occupied non-farm non-res
    "rec_comm_re_owner":     ["ACCT_CH0030"],
    "co_comm_re_nonown":     ["ACCT_CH0031"],      # Non-owner-occupied non-farm non-res
    "rec_comm_re_nonown":    ["ACCT_CH0032"],
    # Commercial non-RE sub-categories
    "co_comm_nonre_ag":      ["ACCT_CH0033"],      # Agricultural production
    "rec_comm_nonre_ag":     ["ACCT_CH0034"],
    "co_comm_nonre_ci":      ["ACCT_CH0035"],      # Commercial & industrial
    "rec_comm_nonre_ci":     ["ACCT_CH0036"],
    "co_comm_nonre_unsec":   ["ACCT_CH0037"],      # Unsecured commercial
    "rec_comm_nonre_unsec":  ["ACCT_CH0038"],
    "co_comm_nonre_rev":     ["ACCT_CH0039"],      # Unsecured revolving commercial
    "rec_comm_nonre_rev":    ["ACCT_CH0040"],

    # ── Shares & Deposits breakdown (FS220.txt) ──────────────────────────────
    "share_drafts":         ["ACCT_902"],         # Share Drafts
    "regular_shares":       ["ACCT_657"],         # Regular Shares
    "money_market_shares":  ["ACCT_911"],         # Money Market Shares
    "share_certificates":   ["ACCT_908C"],        # Share Certificates (total)
    "ira_keogh":            ["ACCT_906A"],        # IRA/KEOGH Accounts
    "total_shares_no_nm":   ["ACCT_013"],         # Total Shares excl. non-member deposits
    "non_member_deposits":  ["ACCT_880"],         # Non-Member Deposits
    "total_borrowings":     ["ACCT_860C"],        # Total Borrowings (FS220)

    # ── Loan breakdown (FS220A.txt, FS220H.txt, FS220L.txt) ──────────────────
    "loan_credit_card":     ["ACCT_396"],         # Unsecured Credit Card Loans (FS220A)
    "loan_pal":             ["ACCT_397A"],        # Payday Alternative Loans (FS220H)
    "loan_student":         ["ACCT_698A"],        # Non-Federally Guaranteed Student Loans (FS220H)
    "loan_other_unsecured": ["ACCT_397"],         # All Other Unsecured Loans/Lines of Credit (FS220A)
    "loan_new_vehicle":     ["ACCT_385"],         # New Vehicle Loans (FS220A)
    "loan_used_vehicle":    ["ACCT_370"],         # Used Vehicle Loans (FS220A)
    "loan_leases":          ["ACCT_002"],         # Leases Receivable (FS220)
    "loan_other_secured":   ["ACCT_698C"],        # All Other Secured Non-RE Loans (FS220L)
    "loan_re_1st_lien":     ["ACCT_703A"],        # 1-4 Family RE 1st Lien (FS220L)
    "loan_re_junior_lien":  ["ACCT_386A"],        # 1-4 Family RE Junior Lien (FS220L)
    "unfunded_re_junior_lien": ["ACCT_811D"],     # Unfunded commitments - Revolving Open-End 1-4 Family RE (FS220M)
    "loan_re_other":        ["ACCT_386B"],        # All Other Non-Commercial RE (FS220L)
    "loan_commercial_re":   ["ACCT_718A5"],       # Commercial RE Secured (FS220L)
    "loan_commercial_nonre":["ACCT_400P"],        # Commercial Not RE Secured (FS220L)
    "total_loans":          ["ACCT_025B1", "ACCT_025B"],  # Total Loans & Leases
}

# ─────────────────────────────────────────────────────────────────────────────
# Ratio definitions (label, format, benchmark, direction)
# ─────────────────────────────────────────────────────────────────────────────

RATIOS: dict[str, dict] = {
    "net_worth_ratio": {
        "label":       "Net Worth Ratio",
        "desc":        "Net Worth ÷ Total Assets",
        "fmt":         ".2%",
        "direction":   "higher",   # higher is better
        "benchmark":   0.07,       # NCUA "well-capitalized" threshold
        "bm_label":    "7% (well-capitalized)",
    },
    "roa": {
        "label":       "Return on Assets (ROA)",
        "desc":        "Annualised Net Income ÷ Total Assets",
        "fmt":         ".3%",
        "direction":   "higher",
        "benchmark":   0.006,
        "bm_label":    "0.60%",
    },
    "loan_to_share": {
        "label":       "Loan-to-Share Ratio",
        "desc":        "Total Loans ÷ Total Shares & Deposits",
        "fmt":         ".1%",
        "direction":   "neutral",
        "benchmark":   0.80,
        "bm_label":    "~80%",
    },
    "delinquency_ratio": {
        "label":       "Delinquency Ratio",
        "desc":        "Delinquent Loans (2+ mo.) ÷ Total Loans",
        "fmt":         ".2%",
        "direction":   "lower",
        "benchmark":   0.01,
        "bm_label":    "1.00%",
    },
    "charge_off_ratio": {
        "label":       "Net Charge-off Ratio",
        "desc":        "Annualised Net Charge-offs ÷ Total Loans",
        "fmt":         ".3%",
        "direction":   "lower",
        "benchmark":   0.005,
        "bm_label":    "0.50%",
    },
    "opex_ratio": {
        "label":       "Operating Expense Ratio",
        "desc":        "Annualised Operating Expenses ÷ Total Assets",
        "fmt":         ".2%",
        "direction":   "lower",
        "benchmark":   0.04,
        "bm_label":    "4.00%",
    },
    "asset_growth": {
        "label":       "Asset Growth (Ann.)",
        "desc":        "Quarter-over-quarter change in Total Assets × 4",
        "fmt":         ".1%",
        "direction":   "higher",
        "benchmark":   0.05,
        "bm_label":    "5%",
    },
    "share_growth": {
        "label":       "Share Growth (Ann.)",
        "desc":        "Quarter-over-quarter change in Total Shares × 4",
        "fmt":         ".1%",
        "direction":   "higher",
        "benchmark":   0.05,
        "bm_label":    "5%",
    },
    "net_interest_margin": {
        "label":       "Net Interest Margin (NIM)",
        "desc":        "Annualised (Interest Income − Funding Costs) ÷ Total Assets",
        "fmt":         ".2%",
        "direction":   "higher",
        "benchmark":   0.03,
        "bm_label":    "3.00%",
    },
    "efficiency_ratio": {
        "label":       "Efficiency Ratio",
        "desc":        "Operating Expense ÷ (Net Interest Income + Non-Interest Income)",
        "fmt":         ".1%",
        "direction":   "lower",
        "benchmark":   0.75,
        "bm_label":    "75%",
    },
    "investment_yield": {
        "label":       "Investment Yield",
        "desc":        "Annualised Investment Income ÷ Total Investment Portfolio",
        "fmt":         ".2%",
        "direction":   "higher",
        "benchmark":   0.03,
        "bm_label":    "3.00%",
    },
    "cost_of_funds": {
        "label":       "Cost of Funds",
        "desc":        "Annualised Dividends & Interest Expense ÷ Total Shares & Deposits",
        "fmt":         ".2%",
        "direction":   "lower",
        "benchmark":   0.015,
        "bm_label":    "1.50%",
    },
    "liquidity_ratio": {
        "label":       "Liquidity Ratio",
        "desc":        "(Cash + Investments < 1 yr) ÷ Total Assets",
        "fmt":         ".1%",
        "direction":   "higher",
        "benchmark":   0.05,
        "bm_label":    "5%",
    },
    "interest_income_ann": {
        "label":       "Interest Income (Ann.)",
        "desc":        "Annualised Loan & Investment Interest Income",
        "fmt":         ",.0f",
        "prefix":      "$",
        "direction":   "neutral",
        "benchmark":   None,
        "bm_label":    "—",
    },
    "nonint_income_ann": {
        "label":       "Non-Interest Income (Ann.)",
        "desc":        "Annualised Non-Interest Income (NCUA ACCT_117)",
        "fmt":         ",.0f",
        "prefix":      "$",
        "direction":   "neutral",
        "benchmark":   None,
        "bm_label":    "—",
    },
    "net_income": {
        "label":       "Net Income (Ann.)",
        "desc":        "Annualised Net Income (Loss)",
        "fmt":         ",.0f",
        "prefix":      "$",
        "direction":   "higher",
        "benchmark":   0,
        "bm_label":    "> $0",
    },
}

# ─────────────────────────────────────────────────────────────────────────────
# Field-of-membership lookup (NCUA TOM_CODE → human-readable description)
# ─────────────────────────────────────────────────────────────────────────────
TOM_DESC: dict[str, str] = {
    "00": "Community Charter",
    "01": "Religious / Church Groups",
    "02": "Ethnic / Cultural Organizations",
    "03": "Fraternal / Service Organizations",
    "04": "Educational Employees",
    "05": "Hospital / Healthcare Workers",
    "06": "Postal Employees",
    "15": "Military / Veterans",
    "20": "Manufacturing Employees",
    "21": "Transportation Workers",
    "22": "Retail / Service Industry",
    "23": "Construction / Trades",
    "24": "Agricultural Workers",
    "34": "Teachers / School Employees",
    "35": "County / Municipal Employees",
    "36": "Government / Public Safety",
    "40": "Credit Union Organization Members",
    "41": "Cooperative / Association Members",
    "42": "Professional Association",
    "43": "Labor Union Members",
    "44": "Financial / Insurance Industry",
    "49": "Community Development",
    "50": "Technology / Telecom Workers",
    "51": "Energy / Utilities Workers",
    "52": "Healthcare Workers",
    "53": "Utility / Telecom Employees",
    "54": "Multiple Select Employee Groups",
    "66": "Rural / Agricultural Community",
    "98": "Multiple Common Bond",
    "99": "Community Charter",
}

CU_TYPE_DESC: dict[str, str] = {
    "1": "Federal Credit Union",
    "2": "Federally Insured State Credit Union",
    "3": "State Credit Union",
}

# Traffic-light colours
GREEN   = "#27ae60"
YELLOW  = "#f39c12"
RED     = "#e74c3c"
NEUTRAL = "#3498db"
DARK    = "#2c3e50"

# ═══════════════════════════════════════════════════════════════════════════════
# UPSTART RECOMMENDATION MODULE  (drew3 addition)
# To revert to drew2 behaviour, delete everything between the two rows of ═══
# and remove the four "# DREW3:" lines in build_dashboard() and main().
# ═══════════════════════════════════════════════════════════════════════════════

UPSTART_PERSONAL_YIELD   = 0.065   # 6.5 % net after losses & fees, before cost of funds
UPSTART_AUTO_YIELD       = 0.055   # 5.5 % net after losses & fees, before cost of funds
UPSTART_HELOC_YIELD      = 0.055   # 5.5 % net after losses & fees, before cost of funds
UPSTART_AUTO_HELOC_YIELD = 0.055   # alias kept for HTML builder references

DEALER_FEE_AUTO          = 0.010   # 1.0 % flat dealer participation fee deducted from auto APR

_CONF_HIGH_SCORE   = 5
_CONF_MEDIUM_SCORE = 2
_CONF_HIGH_DATA    = 5
_CONF_MEDIUM_DATA  = 3


def compute_upstart_recommendation(
    cu_name: str,
    ratio_rows: list[dict],
    cur_loans: Optional[dict] = None,
    cur_losses: Optional[dict] = None,
    cur_rates: Optional[dict] = None,
) -> dict:
    """
    Analyse all dashboard metrics and produce an Upstart product recommendation.

    Returns a dict with keys:
      overall, confidence, products, product_reasoning,
      signals, concerns, rationale, score, data_points
    """
    if not ratio_rows:
        return {
            "overall": "Insufficient Data",
            "confidence": "Low",
            "products": [],
            "product_reasoning": {},
            "signals": [],
            "concerns": ["No financial data available for analysis"],
            "rationale": "Insufficient financial data to make a recommendation.",
            "score": 0,
            "data_points": 0,
        }

    cur  = ratio_rows[-1]
    prev = ratio_rows[-2] if len(ratio_rows) >= 2 else None

    score       = 0
    data_pts    = 0
    signals: list[tuple[int, str]] = []   # (weight, text) — sorted by weight at return
    concerns: list[str] = []
    want_personal = False
    want_auto     = False
    want_heloc    = False
    personal_score = 0   # per-product impact scores for ranking
    auto_score     = 0
    heloc_score    = 0

    # ── 1. ROA ────────────────────────────────────────────────────────────────
    roa = cur.get("roa")
    if roa is not None:
        data_pts += 1
        if roa < 0.003:
            score += 3
            personal_score += 3
            signals.append((3,
                f"ROA of {roa:.3%} is well below the 0.60% benchmark — Upstart "
                "loans could materially lift earnings"
            ))
        elif roa < 0.006:
            score += 2
            personal_score += 2
            signals.append((2,
                f"ROA of {roa:.3%} is below the 0.60% benchmark — incremental "
                "yield from Upstart loans would be accretive"
            ))
        elif roa >= 0.010:
            score -= 1
            concerns.append(
                f"ROA of {roa:.3%} is already strong — Upstart would be additive "
                "but less urgently needed"
            )
        if prev:
            prev_roa = prev.get("roa")
            if prev_roa is not None and roa < prev_roa - 0.001:
                score += 1
                personal_score += 1
                signals.append((1, "ROA shows a declining trend — proactive yield improvement is advisable"))

    # ── 2. Net Interest Margin ────────────────────────────────────────────────
    nim = cur.get("net_interest_margin")
    if nim is not None:
        data_pts += 1
        if nim < 0.025:
            score += 3
            personal_score += 3
            want_personal = True
            signals.append((3,
                f"NIM of {nim:.2%} is well below the 3.00% benchmark — "
                "higher-yielding Upstart personal loans could significantly widen the margin"
            ))
        elif nim < 0.030:
            score += 2
            personal_score += 2
            want_personal = True
            signals.append((2,
                f"NIM of {nim:.2%} is below the 3.00% benchmark — Upstart "
                f"personal loans at {UPSTART_PERSONAL_YIELD:.1%} net would help"
            ))
        elif nim >= 0.040:
            # Suppress the "healthy NIM" concern when investment income is a large
            # contributor — Section 13 will flag the weaker loan-only NIM instead.
            nim_ex = cur.get("nim_ex_investments")
            if nim_ex is None or (nim - nim_ex) <= 0.005:
                concerns.append(
                    f"NIM of {nim:.2%} is already healthy — Upstart would still add yield"
                )

    # ── 3. Loan-to-Share ratio ────────────────────────────────────────────────
    lts = cur.get("loan_to_share")
    if lts is not None:
        data_pts += 1
        if lts < 0.60:
            score += 3
            personal_score += 3; auto_score += 3; heloc_score += 3
            want_personal = True
            want_auto     = True
            want_heloc    = True
            signals.append((3,
                f"Loan-to-share of {lts:.1%} is well below ~80% — significant "
                "idle deposits await deployment via Upstart"
            ))
        elif lts < 0.75:
            score += 2
            personal_score += 2
            want_personal = True
            signals.append((2,
                f"Loan-to-share of {lts:.1%} is below the ~80% optimum — "
                "Upstart loan volume could improve asset utilisation"
            ))
        elif lts > 0.95:
            score -= 2
            concerns.append(
                f"Loan-to-share of {lts:.1%} is very high — limited capacity "
                "for new volume without additional funding"
            )

    # ── 4. Investment Yield ───────────────────────────────────────────────────
    inv_yield = cur.get("investment_yield")
    if inv_yield is not None:
        data_pts += 1
        if inv_yield < UPSTART_AUTO_HELOC_YIELD:
            score += 2
            personal_score += 2; auto_score += 2; heloc_score += 2
            want_personal = True
            want_auto     = True
            want_heloc    = True
            signals.append((2,
                f"Investment yield of {inv_yield:.2%} is below all three Upstart product yields "
                f"(Personal {UPSTART_PERSONAL_YIELD:.1%}, Auto {UPSTART_AUTO_YIELD:.1%}, "
                f"HELOC {UPSTART_HELOC_YIELD:.1%}) net — redeployment into Upstart loans improves return"
            ))
        elif inv_yield < UPSTART_PERSONAL_YIELD:
            score += 1
            personal_score += 1
            want_personal = True
            signals.append((1,
                f"Investment yield of {inv_yield:.2%} is below Upstart personal "
                f"loan yield ({UPSTART_PERSONAL_YIELD:.1%}) — personal loans offer better return"
            ))

    # ── 5. Cost of Funds ─────────────────────────────────────────────────────
    cof = cur.get("cost_of_funds")
    if cof is not None:
        data_pts += 1
        if cof > 0.025:
            score += 2
            personal_score += 2
            signals.append((2,
                f"Elevated cost of funds ({cof:.2%}) requires higher-yielding assets — "
                f"Upstart personal loans provide a ~{UPSTART_PERSONAL_YIELD - cof:.2%} net spread"
            ))
        elif cof > 0.015:
            score += 1
            personal_score += 1
            signals.append((1,
                f"Cost of funds of {cof:.2%} — Upstart personal loans offer a "
                f"~{UPSTART_PERSONAL_YIELD - cof:.2%} net spread"
            ))

    # ── 6. Efficiency Ratio ───────────────────────────────────────────────────
    eff = cur.get("efficiency_ratio")
    if eff is not None:
        data_pts += 1
        if eff > 0.85:
            score += 2
            personal_score += 2
            signals.append((2,
                f"Efficiency ratio of {eff:.1%} is above 85% — Upstart loan "
                "income would improve overhead coverage"
            ))
        elif eff > 0.75:
            score += 1
            personal_score += 1
            signals.append((1,
                f"Efficiency ratio of {eff:.1%} exceeds the 75% benchmark — "
                "incremental revenue from Upstart helps"
            ))

    # ── 7. Net Worth Ratio ────────────────────────────────────────────────────
    nwr = cur.get("net_worth_ratio")
    if nwr is not None:
        data_pts += 1
        if nwr < 0.07:
            score -= 1
            concerns.append(
                f"Net worth ratio of {nwr:.2%} is below the well-capitalised "
                "threshold — capital constraints may limit new loan volume"
            )
        elif nwr >= 0.10:
            score += 1
            personal_score += 1
            signals.append((1,
                f"Net worth ratio of {nwr:.2%} indicates strong capital — "
                "ample room to absorb Upstart loan volume"
            ))

    # ── 8. Delinquency ────────────────────────────────────────────────────────
    dq = cur.get("delinquency_ratio")
    if dq is not None:
        data_pts += 1
        if dq > 0.02:
            concerns.append(
                f"Delinquency ratio of {dq:.2%} is elevated — Upstart's AI "
                "underwriting may actually reduce incremental credit risk versus "
                "traditional origination in this segment"
            )
        elif dq <= 0.010:
            signals.append((0,
                f"Low delinquency of {dq:.2%} reflects strong credit discipline — "
                "Upstart's model complements this approach"
            ))

    # ── 9. Charge-off Rate ────────────────────────────────────────────────────
    co = cur.get("charge_off_ratio")
    if co is not None:
        if co > 0.010:
            concerns.append(
                f"Charge-off ratio of {co:.3%} is above average — confirm Upstart's "
                "loss estimates are factored into yield expectations"
            )

    # ── 10. Loan portfolio composition & diversification ─────────────────────
    if cur_loans:
        total_l = cur_loans.get("total_loans") or 0.0
        if total_l > 0:
            unsecured = sum(
                cur_loans.get(k) or 0.0
                for k in ("loan_credit_card", "loan_pal",
                          "loan_student", "loan_other_unsecured")
            )
            auto = sum(
                cur_loans.get(k) or 0.0
                for k in ("loan_new_vehicle", "loan_used_vehicle", "loan_leases")
            )
            re_total = sum(
                cur_loans.get(k) or 0.0
                for k in ("loan_re_1st_lien", "loan_re_junior_lien",
                          "loan_re_other", "loan_commercial_re")
            )
            unsec_pct = unsecured / total_l
            auto_pct  = auto / total_l
            re_pct    = re_total / total_l

            if unsec_pct < 0.05:
                score += 1
                personal_score += 1
                want_personal = True
                signals.append((1,
                    f"Unsecured personal loans are only {unsec_pct:.1%} of the portfolio — "
                    "Upstart Unsecured Personal would add a high-yield segment and improve diversification"
                ))
            elif unsec_pct > 0.30:
                score += 1
                auto_score += 1; heloc_score += 1
                want_auto  = True
                want_heloc = True
                signals.append((1,
                    f"Unsecured loans are {unsec_pct:.1%} of portfolio — Upstart Auto Retail "
                    "or HELOC would balance concentration and reduce credit risk in this segment"
                ))

            if auto_pct < 0.10:
                score += 1
                auto_score += 1
                want_auto = True
                signals.append((1,
                    f"Auto loans are only {auto_pct:.1%} of portfolio — Upstart Auto Retail "
                    "would grow this underweighted segment and improve yield mix"
                ))
            elif auto_pct > 0.40:
                score += 1
                personal_score += 1; heloc_score += 1
                want_personal = True
                want_heloc    = True
                signals.append((1,
                    f"Auto loans are {auto_pct:.1%} of portfolio — Upstart Unsecured Personal "
                    "or HELOC would reduce auto concentration and diversify interest income"
                ))

            if re_pct > 0.50:
                score += 1
                personal_score += 1; auto_score += 1
                want_personal = True
                want_auto     = True
                signals.append((1,
                    f"Real estate loans represent {re_pct:.1%} of portfolio — Upstart "
                    "Unsecured Personal and Auto Retail would reduce RE concentration and "
                    "improve overall yield"
                ))

    # ── 11. HELOC utilization (Upstart 80% draw-at-origination advantage) ────
    if cur_loans:
        heloc_funded   = cur_loans.get("loan_re_junior_lien") or 0.0
        heloc_unfunded = cur_loans.get("unfunded_re_junior_lien") or 0.0
        heloc_total    = heloc_funded + heloc_unfunded
        if heloc_total > 0:
            data_pts += 1
            heloc_util = heloc_funded / heloc_total
            if heloc_util < 0.50:
                score += 3
                heloc_score += 3
                want_heloc = True
                signals.append((3,
                    f"HELOC utilization is only {heloc_util:.0%} — the existing revolving pool "
                    "is largely idle. Upstart HELOC loans require an 80% draw at origination, "
                    "converting committed capital to earning assets immediately and generating "
                    "significantly more interest income than low-utilization revolving lines"
                ))
            elif heloc_util < 0.75:
                score += 2
                heloc_score += 2
                want_heloc = True
                signals.append((2,
                    f"HELOC utilization of {heloc_util:.0%} is below full deployment. "
                    "Upstart HELOC's required 80% draw at origination puts capital to work "
                    "faster than revolving lines, improving yield on the HELOC portfolio"
                ))
            else:
                signals.append((0,
                    f"HELOC utilization is {heloc_util:.0%} — lines are well-utilized. "
                    "Upstart HELOC's 80% origination draw aligns with existing usage patterns "
                    "and maintains strong earning-asset deployment"
                ))

    # ── 12. Administrative & origination cost savings ─────────────────────────
    # Upstart loans are purchased by the CU fully underwritten — no loan officers,
    # credit review staff, or origination systems required on the CU's side.
    score += 1
    data_pts += 1
    personal_score += 1; auto_score += 1; heloc_score += 1
    signals.append((1,
        "Upstart loans are purchased fully underwritten — no in-house credit review, "
        "loan officer time, or origination system cost required. This reduces effective "
        "cost per loan and improves net yield versus conventionally originated loans"
    ))

    # ── 13. NIM decomposition — investment-propped margin ────────────────────
    # A high overall NIM can mask a weak loan margin when investment yield is
    # unusually elevated. Upstart loans improve the loan-side NIM directly.
    nim     = cur.get("net_interest_margin")
    nim_ex  = cur.get("nim_ex_investments")
    if nim is not None and nim_ex is not None:
        inv_contrib = nim - nim_ex   # percentage-points of NIM supplied by investments
        if inv_contrib > 0.005:      # investments contribute > 50 bps to NIM
            data_pts += 1
            if nim_ex < 0.025:
                score += 3
                personal_score += 3
                want_personal = True
                signals.append((3,
                    f"Overall NIM of {nim:.2%} is heavily supported by investment income "
                    f"(+{inv_contrib:.2%}); loan-only NIM is only {nim_ex:.2%} — well below "
                    "the 3.00% benchmark. Upstart personal loans would directly strengthen "
                    "the lending margin regardless of investment portfolio performance"
                ))
            elif nim_ex < 0.030:
                score += 2
                personal_score += 2
                want_personal = True
                signals.append((2,
                    f"NIM of {nim:.2%} is propped up by {inv_contrib:.2%} of investment "
                    f"income — loan-only NIM is {nim_ex:.2%}, near the 3.00% benchmark. "
                    "Upstart personal loans would improve core lending yield independently "
                    "of the investment portfolio"
                ))
            elif nim_ex < 0.040 and inv_contrib > 0.008:
                score += 1
                personal_score += 1
                want_personal = True
                signals.append((1,
                    f"Investment income contributes {inv_contrib:.2%} to overall NIM of "
                    f"{nim:.2%} — loan-only NIM is {nim_ex:.2%}. Upstart loans would "
                    "diversify and grow interest income beyond what the investment portfolio "
                    "alone provides"
                ))

    # ── 14. Per-product annualised net charge-off analysis ───────────────────
    # cur_losses is {loan_key: annualized_net_co_dollars} from extract_loan_losses().
    # We compute NCO rates by dividing against cur_loans balances.
    if cur_losses and cur_loans:
        total_l = cur_loans.get("total_loans") or 0

        def _nco_rate(loan_key: str) -> Optional[float]:
            bal = cur_loans.get(loan_key) or 0
            net = cur_losses.get(loan_key)
            if net is None or bal <= 0:
                return None
            return net / bal

        # Unsecured personal (CC + other unsecured, excluding PAL/student)
        unsec_co  = sum(cur_losses.get(k) or 0 for k in ("loan_credit_card", "loan_other_unsecured"))
        unsec_bal = sum(cur_loans.get(k)  or 0 for k in ("loan_credit_card", "loan_other_unsecured"))
        unsec_nco = unsec_co / unsec_bal if unsec_bal > 0 else None

        # Vehicle (new + used)
        auto_co  = sum(cur_losses.get(k) or 0 for k in ("loan_new_vehicle", "loan_used_vehicle"))
        auto_bal = sum(cur_loans.get(k)  or 0 for k in ("loan_new_vehicle", "loan_used_vehicle"))
        auto_nco = auto_co / auto_bal if auto_bal > 0 else None

        # Portfolio-wide from product-level data (as a cross-check)
        total_product_co  = sum(v for v in cur_losses.values() if v is not None)
        portfolio_nco_chk = total_product_co / total_l if total_l > 0 else None

        if unsec_nco is not None:
            data_pts += 1
            if unsec_nco > 0.04:
                score += 3
                personal_score += 3
                want_personal = True
                signals.append((3,
                    f"Unsecured loan NCO rate is {unsec_nco:.2%} annualised — well above "
                    "industry norms. Upstart's AI underwriting significantly improves risk "
                    "selection in this segment; switching origination to Upstart would be "
                    "expected to materially reduce losses while maintaining volume"
                ))
            elif unsec_nco > 0.02:
                score += 2
                personal_score += 2
                want_personal = True
                signals.append((2,
                    f"Unsecured loan NCO rate of {unsec_nco:.2%} annualised is elevated. "
                    "Upstart's AI-driven underwriting model has demonstrated lower loss rates "
                    "in the unsecured consumer segment, improving net yield on this book"
                ))
            elif unsec_nco > 0.01:
                score += 1
                personal_score += 1
                want_personal = True
                signals.append((1,
                    f"Unsecured loan NCO rate of {unsec_nco:.2%} annualised is modest but "
                    "leaves room for improvement. Upstart's model targets further loss reduction "
                    "while preserving approval rates"
                ))
            elif unsec_nco >= 0:
                signals.append((0,
                    f"Unsecured loan NCO rate is low at {unsec_nco:.2%} annualised — "
                    "strong credit discipline. Upstart's AI model is designed to maintain "
                    "this standard at scale while expanding origination volume"
                ))

        if auto_nco is not None:
            data_pts += 1
            if auto_nco > 0.015:
                score += 2
                auto_score += 2
                want_auto = True
                signals.append((2,
                    f"Vehicle loan NCO rate of {auto_nco:.2%} annualised is above typical "
                    "auto benchmarks. Upstart Auto Retail's AI underwriting can improve risk "
                    "selection in this segment, reducing losses without sacrificing volume"
                ))
            elif auto_nco > 0.007:
                score += 1
                auto_score += 1
                want_auto = True
                signals.append((1,
                    f"Vehicle loan NCO rate of {auto_nco:.2%} annualised is somewhat elevated. "
                    "Upstart Auto Retail's risk model could incrementally improve selection quality"
                ))
            elif auto_nco >= 0:
                signals.append((0,
                    f"Vehicle loan NCO rate is low at {auto_nco:.2%} annualised — "
                    "solid performance. Upstart Auto Retail would complement this quality "
                    "while adding origination scale"
                ))

        # Net yield comparison for unsecured — Upstart's 6.5% is net of losses,
        # so the correct comparison is (gross unsecured rate − NCO) vs 6.5%.
        if cur_rates and unsec_nco is not None and unsec_bal > 0:
            cc_rate  = cur_rates.get("rate_credit_card") or 0
            oth_rate = cur_rates.get("rate_other_unsecured") or 0
            cc_bal   = cur_loans.get("loan_credit_card") or 0
            oth_bal  = cur_loans.get("loan_other_unsecured") or 0
            if (cc_bal + oth_bal) > 0 and (cc_rate or oth_rate):
                w_unsec_gross = (cc_rate * cc_bal + oth_rate * oth_bal) / (cc_bal + oth_bal)
                net_unsec_yield = w_unsec_gross - unsec_nco
                upstart_adv = UPSTART_PERSONAL_YIELD - net_unsec_yield
                if net_unsec_yield < UPSTART_PERSONAL_YIELD:
                    score += 1
                    personal_score += 1
                    want_personal = True
                    signals.append((1,
                        f"On a net-of-losses basis, existing unsecured loans yield "
                        f"~{net_unsec_yield:.2%} (gross {w_unsec_gross:.2%} minus "
                        f"{unsec_nco:.2%} annualised NCO) — below Upstart Unsecured Personal's "
                        f"{UPSTART_PERSONAL_YIELD:.1%} net-of-losses return. Upstart delivers "
                        f"a ~{upstart_adv:.2%} net yield advantage on a like-for-like basis"
                    ))
                else:
                    signals.append((0,
                        f"Net-of-losses yield on existing unsecured loans is ~{net_unsec_yield:.2%} "
                        f"(gross {w_unsec_gross:.2%} minus {unsec_nco:.2%} NCO) — ahead of "
                        f"Upstart's {UPSTART_PERSONAL_YIELD:.1%} net return. Upstart Personal "
                        f"would complement existing volume rather than replace it on yield alone"
                    ))

        # Flag individual high-loss product categories
        for loan_key, prod_label in (
            ("loan_re_1st_lien",     "RE 1st Lien"),
            ("loan_re_junior_lien",  "RE Junior Lien / HELOC"),
            ("loan_commercial_re",   "Commercial RE"),
            ("loan_commercial_nonre","Commercial Non-RE"),
        ):
            rate = _nco_rate(loan_key)
            if rate is not None and rate > 0.005:
                concerns.append(
                    f"{prod_label} NCO rate of {rate:.2%} annualised is notable — "
                    "adding Upstart consumer or auto loans would diversify away from this "
                    "higher-loss concentration"
                )

    # ── 15. Auto loan dealer fee drag ────────────────────────────────────────
    # Dealer-sourced new/used auto loans carry a ~1% participation fee paid to
    # the originating dealer, reducing the CU's net yield by DEALER_FEE_AUTO.
    # Upstart Auto Retail loans arrive fully underwritten with no dealer fee,
    # so the comparison is: effective existing auto yield vs Upstart auto yield.
    if cur_rates and cur_loans:
        new_rate  = cur_rates.get("rate_new_auto")
        used_rate = cur_rates.get("rate_used_auto")
        new_bal   = cur_loans.get("loan_new_vehicle") or 0
        used_bal  = cur_loans.get("loan_used_vehicle") or 0
        total_auto_bal = new_bal + used_bal

        if total_auto_bal > 0 and (new_rate or used_rate):
            # Weighted average gross auto APR
            weighted_gross = (
                ((new_rate  or 0) * new_bal + (used_rate or 0) * used_bal)
                / total_auto_bal
            )
            effective_auto_yield = weighted_gross - DEALER_FEE_AUTO

            # Further reduce by annualised auto NCO if available
            auto_nco_for_fee = None
            if cur_losses:
                auto_co  = sum(cur_losses.get(k) or 0 for k in ("loan_new_vehicle", "loan_used_vehicle"))
                if auto_co and total_auto_bal:
                    auto_nco_for_fee = auto_co / total_auto_bal
                    net_auto_yield = effective_auto_yield - auto_nco_for_fee
                else:
                    net_auto_yield = effective_auto_yield
            else:
                net_auto_yield = effective_auto_yield

            data_pts += 1
            # Upstart Auto Retail's 5.5% is net of losses — the correct comparison
            # is existing auto yield after both dealer fee AND NCO are deducted.
            spread = UPSTART_AUTO_YIELD - net_auto_yield

            if net_auto_yield < UPSTART_AUTO_YIELD:
                score += 2
                auto_score += 2
                want_auto = True
                nco_clause = (
                    f" and the {auto_nco_for_fee:.2%} annualised NCO rate"
                    if auto_nco_for_fee else ""
                )
                signals.append((2,
                    f"Existing auto loans carry a gross weighted APR of {weighted_gross:.2%}. "
                    f"After deducting the 1% dealer participation fee{nco_clause}, the "
                    f"net-of-losses auto yield is ~{net_auto_yield:.2%} — below Upstart Auto "
                    f"Retail's ~{UPSTART_AUTO_YIELD:.1%} net-of-losses return. Upstart Auto "
                    f"carries no dealer fee and its yield is already net of losses, delivering "
                    f"a ~{spread:.2%} net yield advantage on a like-for-like basis"
                ))
            elif net_auto_yield < UPSTART_AUTO_YIELD + 0.005:
                score += 1
                auto_score += 1
                want_auto = True
                nco_clause = (
                    f" and {auto_nco_for_fee:.2%} NCO"
                    if auto_nco_for_fee else ""
                )
                signals.append((1,
                    f"After the 1% dealer fee{nco_clause}, net auto yield is "
                    f"~{net_auto_yield:.2%} — roughly in line with Upstart Auto Retail's "
                    f"~{UPSTART_AUTO_YIELD:.1%} net-of-losses return, but Upstart eliminates "
                    f"dealer relationship overhead and underwriting cost"
                ))
            else:
                nco_clause = (
                    f" and {auto_nco_for_fee:.2%} NCO"
                    if auto_nco_for_fee else ""
                )
                signals.append((0,
                    f"Net auto yield after 1% dealer fee{nco_clause} is ~{net_auto_yield:.2%}, "
                    f"which exceeds Upstart Auto Retail's ~{UPSTART_AUTO_YIELD:.1%} net-of-losses "
                    f"return. Upstart Auto would add scale and simplicity but not a yield uplift "
                    f"versus the existing dealer-sourced book"
                ))

    # ── 16. Wholesale borrowings leverage ────────────────────────────────────
    # A CU with unusually high wholesale borrowings relative to assets has often
    # borrowed aggressively to hit an asset milestone (e.g. $1B, $4B, $10B —
    # thresholds that drive CEO compensation benchmarks and board optics).  The
    # resulting debt service creates pressure to improve portfolio yield, making
    # Upstart's higher-yield, fully-underwritten loans particularly attractive.
    _cur_borr = cur.get("_total_borrowings")
    _cur_ta   = cur.get("_total_assets")
    if _cur_borr is not None and _cur_ta and _cur_ta > 0:
        borr_pct = _cur_borr / _cur_ta
        data_pts += 1
        if borr_pct >= 0.05:          # ≥ 5% of assets — significantly leveraged
            score += 2
            personal_score += 2
            want_personal = True
            nw_ratio = cur.get("net_worth_ratio")
            nw_clause = (
                f" with a net worth ratio of only {nw_ratio:.1%}"
                if nw_ratio is not None and nw_ratio < 0.10
                else ""
            )
            signals.append((2,
                f"Wholesale borrowings represent {borr_pct:.1%} of total assets{nw_clause} — "
                f"well above the industry norm of 1–2%. This level of leverage suggests the "
                f"credit union may have borrowed to reach a strategic asset milestone, and now "
                f"faces pressure to generate sufficient yield to service that debt. "
                f"Upstart's higher-yield, fully-underwritten loans offer an incremental spread "
                f"improvement that directly addresses this earnings gap."
            ))
        elif borr_pct >= 0.03:        # 3–5% — elevated but not extreme
            score += 1
            personal_score += 1
            signals.append((1,
                f"Wholesale borrowings are elevated at {borr_pct:.1%} of total assets "
                f"(industry norm is 1–2%), indicating some reliance on wholesale funding. "
                f"Improving loan portfolio yield through Upstart would strengthen the spread "
                f"above funding costs and reduce dependency on borrowed money."
            ))

    # ── Determine product recommendations — ranked by per-product impact score ─
    _product_items: list[tuple[int, str, str]] = []   # (score, name, reasoning)

    if want_personal or (score >= 2 and not want_auto and not want_heloc):
        _product_items.append((personal_score, "Unsecured Personal",
            f"Upstart Unsecured Personal loans target ~{UPSTART_PERSONAL_YIELD:.1%} net return "
            "after losses and fees (before cost of funds) — the highest-yield Upstart product, "
            "best suited for improving NIM and ROA. Delivered fully underwritten with no "
            "in-house origination cost."
        ))
    if want_auto:
        _product_items.append((auto_score, "Auto Retail",
            f"Upstart Auto Retail targets ~{UPSTART_AUTO_YIELD:.1%} net return "
            "after losses and fees (before cost of funds) — lower risk profile, complements "
            "existing auto lending infrastructure. Delivered pre-underwritten without additional "
            "staffing or systems overhead."
        ))
    if want_heloc:
        _product_items.append((heloc_score, "HELOC",
            f"Upstart HELOC targets ~{UPSTART_HELOC_YIELD:.1%} net return after losses and "
            "fees (before cost of funds). Upstart HELOC loans require an 80% draw at origination, "
            "immediately converting committed capital to earning assets — more revenue than "
            "revolving lines that may sit underutilized."
        ))

    # Sort highest-impact product first
    _product_items.sort(key=lambda x: -x[0])
    products: list[str]               = [name for _, name, _ in _product_items]
    product_reasoning: dict[str, str] = {name: reason for _, name, reason in _product_items}
    product_scores: dict[str, int]    = {name: sc for sc, name, _ in _product_items}

    # ── Overall verdict ───────────────────────────────────────────────────────
    if score >= 6:
        overall = "Yes — Strongly Recommended"
    elif score >= _CONF_HIGH_SCORE:
        overall = "Yes — Recommended"
    elif score >= _CONF_MEDIUM_SCORE:
        overall = "Yes — Potentially Beneficial"
    elif score >= 0:
        overall = "Neutral — Evaluate Case by Case"
    else:
        overall = "Low Priority — Existing Metrics Are Sufficient"

    # ── Confidence ────────────────────────────────────────────────────────────
    if data_pts >= _CONF_HIGH_DATA and abs(score) >= _CONF_HIGH_SCORE:
        confidence = "High"
    elif data_pts >= _CONF_MEDIUM_DATA and abs(score) >= _CONF_MEDIUM_SCORE:
        confidence = "Medium"
    else:
        confidence = "Low"

    # ── Sort signals by weight (highest impact first) ─────────────────────────
    signals_sorted: list[str] = [text for _, text in sorted(signals, key=lambda x: -x[0])]

    # ── Rationale text ────────────────────────────────────────────────────────
    cur_q = cur.get("quarter", "the most recent quarter")
    parts = [
        f"Based on {cu_name}'s NCUA 5300 call report data through {cur_q}, the following "
        "analysis evaluates whether Upstart's AI-powered lending products would improve "
        "overall financial performance."
    ]
    if signals_sorted:
        parts.append("\n**Supporting Factors** (ranked by impact):")
        parts += [f"• {s}" for s in signals_sorted]
    if concerns:
        parts.append("\n**Considerations & Cautions:**")
        parts += [f"• {c}" for c in concerns]
    if products:
        parts.append("\n**Recommended Products** (ranked by applicability and impact):")
        rank_labels = ["Primary", "Secondary", "Also Consider"]
        for i, p in enumerate(products):
            label = rank_labels[i] if i < len(rank_labels) else f"#{i+1}"
            parts.append(f"• **{label} — {p}**: {product_reasoning[p]}")

    cof_note = (
        f"With this credit union's cost of funds at {cof:.2%}, the net spread above funding "
        f"costs would be approximately {UPSTART_PERSONAL_YIELD - cof:.2%} for Unsecured Personal, "
        f"{UPSTART_AUTO_YIELD - cof:.2%} for Auto Retail, and {UPSTART_HELOC_YIELD - cof:.2%} "
        f"for HELOC."
        if cof is not None
        else "Cost of funds data was not available to calculate precise spread above funding costs."
    )
    parts.append(
        f"\n**Return Context:** Upstart Unsecured Personal targets ~{UPSTART_PERSONAL_YIELD:.1%} "
        f"net return after losses and fees; Auto Retail and HELOC target ~{UPSTART_AUTO_YIELD:.1%} "
        f"net. All yields are before cost of funds and delivered without in-house origination cost. "
        f"{cof_note}"
    )

    conf_note = {
        "High":   "High confidence is assigned because multiple key metrics consistently and "
                  "clearly point toward the same conclusion with sufficient data across quarters.",
        "Medium": "Medium confidence reflects that while the data supports the recommendation, "
                  "some metrics are neutral or data coverage is partial.",
        "Low":    "Low confidence reflects limited data availability, mixed metric signals, "
                  "or fewer than three reporting quarters for trend analysis.",
    }[confidence]
    parts.append(f"\n**Confidence Level — {confidence}:** {conf_note}")

    return {
        "overall":           overall,
        "confidence":        confidence,
        "products":          products,
        "product_reasoning": product_reasoning,
        "product_scores":    product_scores,
        "signals":           signals_sorted,
        "signal_weights":    [w for w, _ in sorted(signals, key=lambda x: -x[0])],
        "cur_ratios":        cur,
        "concerns":          concerns,
        "rationale":         "\n".join(parts),
        "score":             score,
        "data_points":       data_pts,
    }


def build_upstart_recommendation_html(rec: dict, cu_name: str) -> str:
    """Build the top-of-dashboard Upstart recommendation banner (drew3)."""
    overall    = rec["overall"]
    confidence = rec["confidence"]
    products   = rec["products"]
    signals    = rec["signals"]
    concerns   = rec["concerns"]

    if "Strongly" in overall or ("Yes" in overall and "Potentially" not in overall and "Recommended" in overall):
        banner_bg  = "#1a6b3c"
        banner_bdr = "#27ae60"
        icon       = "&#10003;&#10003;"
    elif "Potentially" in overall or overall.startswith("Yes"):
        banner_bg  = "#1a5276"
        banner_bdr = "#2980b9"
        icon       = "&#128200;"
    elif "Neutral" in overall:
        banner_bg  = "#7d6608"
        banner_bdr = "#f1c40f"
        icon       = "&#9878;"
    else:
        banner_bg  = "#4a4a4a"
        banner_bdr = "#95a5a6"
        icon       = "&#8505;"

    conf_colors = {"High": "#27ae60", "Medium": "#f39c12", "Low": "#e74c3c"}
    conf_color  = conf_colors.get(confidence, "#95a5a6")

    _rank_labels = ["① Primary", "② Secondary", "③ Also Consider"]
    product_badges = "".join(
        f'<span style="background:rgba(255,255,255,.18);border-radius:6px;'
        f'padding:4px 12px;font-size:.82rem;font-weight:600;margin-right:6px;">'
        f'<span style="opacity:.7;font-size:.75rem;margin-right:4px;">'
        f'{_rank_labels[i] if i < len(_rank_labels) else f"#{i+1}"}&nbsp;·&nbsp;</span>{p}</span>'
        for i, p in enumerate(products)
    ) if products else (
        '<span style="background:rgba(255,255,255,.12);border-radius:6px;'
        'padding:4px 12px;font-size:.82rem;opacity:.8;">No product recommended at this time</span>'
    )

    preview_items = ""
    for s in signals[:3]:
        preview_items += f'<li style="margin-bottom:5px;opacity:.9;font-size:.86rem;">{s}</li>'
    if not signals:
        for c in concerns[:2]:
            preview_items += (
                f'<li style="margin-bottom:5px;opacity:.8;font-size:.86rem;font-style:italic;">'
                f'&#9888; {c}</li>'
            )

    return (
        f'\n<!-- ── UPSTART RECOMMENDATION BANNER (drew3) ──────────────────────────── -->\n'
        f'<div style="background:{banner_bg};border-left:6px solid {banner_bdr};'
        f'border-radius:12px;padding:24px 32px;margin-bottom:26px;color:white;">\n'
        f'  <div style="display:flex;align-items:flex-start;gap:20px;flex-wrap:wrap;">\n'
        f'    <div style="flex:1;min-width:260px;">\n'
        f'      <div style="font-size:.72rem;font-weight:700;text-transform:uppercase;'
        f'letter-spacing:.1em;opacity:.7;margin-bottom:6px;">\n'
        f'        Upstart Partnership Recommendation &mdash; {cu_name}\n'
        f'      </div>\n'
        f'      <div style="font-size:1.55rem;font-weight:700;line-height:1.25;margin-bottom:10px;">\n'
        f'        {icon}&nbsp; {overall}\n'
        f'      </div>\n'
        f'      <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-bottom:14px;">\n'
        f'        <span style="background:rgba(0,0,0,.25);border-radius:6px;padding:4px 12px;font-size:.82rem;">\n'
        f'          Confidence:&nbsp;<strong style="color:{conf_color};">{confidence}</strong>\n'
        f'        </span>\n'
        f'        {product_badges}\n'
        f'      </div>\n'
        f'      <ul style="list-style:none;margin:0;padding:0;line-height:1.7;">\n'
        f'        {preview_items}\n'
        f'      </ul>\n'
        f'    </div>\n'
        f'    <div style="text-align:right;font-size:.75rem;opacity:.55;align-self:flex-end;">\n'
        f'      Scroll to bottom for full rationale\n'
        f'    </div>\n'
        f'  </div>\n'
        f'</div>\n'
        f'<!-- ── END UPSTART RECOMMENDATION BANNER ───────────────────────────────── -->\n'
    )


def build_upstart_rationale_html(rec: dict) -> str:
    """Build the bottom-of-dashboard Upstart rationale card (drew3)."""
    overall   = rec["overall"]
    confidence = rec["confidence"]
    signals   = rec["signals"]
    concerns  = rec["concerns"]
    products  = rec["products"]
    rationale = rec["rationale"]

    conf_colors = {"High": "#27ae60", "Medium": "#f39c12", "Low": "#e74c3c"}
    conf_bg     = {"High": "#eafaf1", "Medium": "#fef9e7", "Low": "#fdedec"}
    cc = conf_colors.get(confidence, "#95a5a6")
    cb = conf_bg.get(confidence, "#f8f9fa")

    def _impact_badge(weight: int) -> str:
        if weight >= 3:
            return ('<span style="display:inline-block;font-size:.65rem;font-weight:700;'
                    'text-transform:uppercase;letter-spacing:.05em;background:#e8f8f0;'
                    'color:#1a6b3c;border:1px solid #a9dfbf;border-radius:3px;'
                    'padding:1px 5px;margin-right:6px;vertical-align:middle;">High</span>')
        if weight == 2:
            return ('<span style="display:inline-block;font-size:.65rem;font-weight:700;'
                    'text-transform:uppercase;letter-spacing:.05em;background:#fef9e7;'
                    'color:#9a7d0a;border:1px solid #f9e79f;border-radius:3px;'
                    'padding:1px 5px;margin-right:6px;vertical-align:middle;">Med</span>')
        if weight == 1:
            return ('<span style="display:inline-block;font-size:.65rem;font-weight:700;'
                    'text-transform:uppercase;letter-spacing:.05em;background:#eaf4fb;'
                    'color:#1a5276;border:1px solid #a9cce3;border-radius:3px;'
                    'padding:1px 5px;margin-right:6px;vertical-align:middle;">Low</span>')
        return ''  # weight=0: informational, no badge

    signal_weights = rec.get("signal_weights", [])

    def _rows(items: list[str], icon: str, color: str,
              weights: Optional[list[int]] = None) -> str:
        return "".join(
            f'<li style="padding:6px 0;border-bottom:1px solid #f0f3f6;font-size:.88rem;'
            f'color:#34495e;line-height:1.6;">'
            f'<span style="color:{color};margin-right:6px;">{icon}</span>'
            f'{_impact_badge(weights[i]) if weights and i < len(weights) else ""}'
            f'{item}</li>'
            for i, item in enumerate(items)
        )

    empty_signal_li = '<li style="color:#95a5a6;">None identified</li>'
    signal_rows_html = _rows(signals, "&#10003;", "#27ae60", weights=signal_weights) or empty_signal_li

    sig_col = (
        f'<div style="flex:1;min-width:260px;">'
        f'<p style="font-size:.78rem;font-weight:700;text-transform:uppercase;'
        f'letter-spacing:.06em;color:#27ae60;margin-bottom:8px;">'
        f'Supporting Factors <span style="font-weight:400;font-style:italic;'
        f'text-transform:none;letter-spacing:0;">&mdash; ranked by impact</span></p>'
        f'<ul style="list-style:none;margin:0;padding:0;">'
        f'{signal_rows_html}'
        f'</ul></div>'
    ) if signals else ""

    con_col = (
        f'<div style="flex:1;min-width:260px;">'
        f'<p style="font-size:.78rem;font-weight:700;text-transform:uppercase;'
        f'letter-spacing:.06em;color:#e67e22;margin-bottom:8px;">Considerations &amp; Cautions</p>'
        f'<ul style="list-style:none;margin:0;padding:0;">'
        f'{_rows(concerns, "&#9888;", "#e67e22")}'
        f'</ul></div>'
    ) if concerns else ""

    _rank_labels_long = ["Primary Recommendation", "Secondary Recommendation", "Also Consider"]
    _rank_colors = ["#1a6b3c", "#1a5276", "#6c3483"]
    prod_cards = ""
    product_scores = rec.get("product_scores", {})
    for i, p in enumerate(products):
        pr    = rec["product_reasoning"].get(p, "")
        rlbl  = _rank_labels_long[i] if i < len(_rank_labels_long) else f"#{i+1}"
        rclr  = _rank_colors[i] if i < len(_rank_colors) else "#2c3e50"
        prod_cards += (
            f'<div style="flex:1;min-width:220px;background:#f8f9fa;border-radius:8px;'
            f'padding:14px 18px;border-left:4px solid {rclr};">'
            f'<p style="font-size:.7rem;font-weight:700;text-transform:uppercase;'
            f'letter-spacing:.08em;color:{rclr};margin-bottom:4px;">{rlbl}</p>'
            f'<p style="font-weight:700;color:#2c3e50;margin-bottom:6px;">{p}</p>'
            f'<p style="font-size:.86rem;color:#555;line-height:1.6;">{pr}</p>'
            f'</div>'
        )
    prod_section = (
        f'<p style="font-size:.78rem;font-weight:700;text-transform:uppercase;'
        f'letter-spacing:.06em;color:#3498db;margin-bottom:10px;">'
        f'Recommended Products — Ranked by Applicability &amp; Impact</p>'
        f'<div style="display:flex;gap:16px;flex-wrap:wrap;margin-bottom:24px;">{prod_cards}</div>'
    ) if prod_cards else ""

    return (
        f'\n<!-- ── UPSTART RATIONALE CARD (drew3) ─────────────────────────────────── -->\n'
        f'<div class="card" id="upstart-rationale">\n'
        f'  <h2>Upstart Partnership Analysis &amp; Rationale\n'
        f'    <span class="badge">drew3 &middot; Upstart Module</span>\n'
        f'  </h2>\n'
        f'  <div style="display:inline-flex;align-items:center;gap:8px;background:{cb};'
        f'border:1px solid {cc};border-radius:8px;padding:8px 16px;margin-bottom:20px;">\n'
        f'    <span style="font-size:.78rem;font-weight:700;text-transform:uppercase;'
        f'letter-spacing:.06em;color:{cc};">{confidence} Confidence</span>\n'
        f'    <span style="color:#6c7a89;font-size:.82rem;">&mdash;</span>\n'
        f'    <span style="font-size:.88rem;color:#34495e;font-weight:600;">{overall}</span>\n'
        f'  </div>\n'
        f'  <div style="display:flex;gap:32px;flex-wrap:wrap;margin-bottom:24px;">'
        f'{sig_col}{con_col}</div>\n'
        f'  {prod_section}\n'
        f'  <div style="border-top:2px solid #eef1f5;padding-top:18px;">\n'
        f'    <p style="font-size:.78rem;font-weight:700;text-transform:uppercase;'
        f'letter-spacing:.06em;color:#6c7a89;margin-bottom:12px;">Full Analytical Rationale</p>\n'
        f'    <div class="analysis">{_md(rationale)}</div>\n'
        f'  </div>\n'
        f'  <p class="source-note" style="margin-top:14px;">\n'
        f'    Upstart yields are illustrative targets (~{UPSTART_PERSONAL_YIELD:.1%} Unsecured Personal, '
        f'~{UPSTART_AUTO_YIELD:.1%} Auto Retail, ~{UPSTART_HELOC_YIELD:.1%} HELOC) net of losses '
        f'and fees, before cost of funds. All products are delivered fully underwritten with no '
        f'in-house origination cost. HELOC loans require an 80% draw at origination. '
        f'Actual results depend on credit-union-specific factors, Upstart program terms, and '
        f'prevailing market conditions. Not investment or regulatory advice.\n'
        f'  </p>\n'
        f'</div>\n'
        f'<!-- ── END UPSTART RATIONALE CARD ───────────────────────────────────────── -->\n'
    )

def build_sales_questions_html(rec: dict, cu_name: str) -> str:
    """
    Generate a prioritised list of discovery questions for Upstart salespeople.
    Questions are framed as genuine curiosity — they surface the CU's financial
    context without referencing metrics or implying weakness directly.
    For internal use only; not intended to be shared with the credit union.
    """
    cur = rec.get("cur_ratios", {})
    products = rec.get("products", [])

    roa       = cur.get("roa")
    nim       = cur.get("net_interest_margin")
    nim_ex    = cur.get("nim_ex_investments")
    lts       = cur.get("loan_to_share")
    eff       = cur.get("efficiency_ratio")
    cof       = cur.get("cost_of_funds")
    nwr       = cur.get("net_worth_ratio")
    inv_yield = cur.get("investment_yield")
    dq        = cur.get("delinquency_ratio")
    co        = cur.get("charge_off_ratio")
    borr_pct  = (
        cur.get("_total_borrowings") / cur.get("_total_assets")
        if cur.get("_total_borrowings") and cur.get("_total_assets")
        else None
    )

    # Each entry: (priority 0–3, question_text, coaching_note)
    # Priority mirrors signal weights: 3=high, 2=med, 1=low, 0=always-include
    items: list[tuple[int, str, str]] = []

    # ── Earnings pressure ────────────────────────────────────────────────────
    if roa is not None and roa < 0.006:
        pri = 3 if roa < 0.003 else 2
        items.append((pri,
            "How is your board thinking about return on assets for the coming year — "
            "is there a specific earnings target you're working toward, and which levers "
            "are you most focused on to get there?",
            f"ROA is {roa:.2%} — below typical CU benchmarks. Surfaces earnings pressure "
            "without mentioning the ratio. Naturally leads to yield improvement strategies."
        ))

    # ── Net interest margin ──────────────────────────────────────────────────
    if nim is not None and nim < 0.030:
        pri = 3 if nim < 0.025 else 2
        items.append((pri,
            "As rates have moved around over the past couple of years, how has your "
            "net interest margin been holding up — and where do you see the biggest "
            "opportunity to widen your spread from here?",
            f"NIM is {nim:.2%}, below the 3% benchmark. Opens the door to discussing "
            "higher-yield loan products without labelling the margin as weak."
        ))

    # ── Loan-only NIM propped by investments ─────────────────────────────────
    if nim is not None and nim_ex is not None and (nim - nim_ex) > 0.005 and nim_ex < 0.035:
        items.append((2,
            "When you strip out investment income and look purely at what your loan "
            "portfolio is earning, how does that lending margin compare to where you'd "
            "ideally like it? Is growing loan yield a strategic priority right now?",
            f"Loan-only NIM is {nim_ex:.2%} — the overall NIM is flattered by "
            f"{nim - nim_ex:.2%} of investment income. Focuses the conversation on "
            "the lending margin specifically."
        ))

    # ── Idle deposits / loan-to-share ────────────────────────────────────────
    if lts is not None and lts < 0.75:
        pri = 3 if lts < 0.60 else 2
        items.append((pri,
            "You've clearly been growing your deposit base — how are you thinking "
            "about putting that liquidity to work productively? Are there loan "
            "categories you feel like you haven't fully penetrated yet?",
            f"Loan-to-share is {lts:.1%}, well below the ~80% optimum. Frames idle "
            "deposits as a growth opportunity rather than an imbalance."
        ))

    # ── Efficiency ───────────────────────────────────────────────────────────
    if eff is not None and eff > 0.75:
        pri = 2 if eff > 0.85 else 1
        items.append((pri,
            "As you look at your cost structure versus revenue, where do you see "
            "the most leverage to improve operating efficiency — is it primarily "
            "on the revenue side, the expense side, or a combination?",
            f"Efficiency ratio is {eff:.1%}, above the 75% benchmark. Opens both "
            "sides of the equation; Upstart addresses the revenue side through "
            "higher-yield volume with zero origination overhead."
        ))

    # ── Investment yield vs. loan yields ────────────────────────────────────
    if inv_yield is not None and inv_yield < UPSTART_AUTO_YIELD:
        items.append((2,
            "How do you think about the balance between your investment portfolio "
            "and growing your loan book? At today's spreads, do you find yourself "
            "wishing more of that capital was deployed in loans instead?",
            f"Investment yield is {inv_yield:.2%} — below all three Upstart product "
            "yields. Surfaces the opportunity cost of the investment portfolio "
            "without suggesting the CU is mismanaged."
        ))
    elif inv_yield is not None and inv_yield < UPSTART_PERSONAL_YIELD:
        items.append((1,
            "How do you think about the balance between your investment portfolio "
            "and growing your loan book? At today's spreads, do you find yourself "
            "wishing more of that capital was deployed in consumer loans instead?",
            f"Investment yield is {inv_yield:.2%} — below Upstart Personal's target "
            "net yield. Opens the loan-vs-investment redeployment conversation."
        ))

    # ── Cost of funds ────────────────────────────────────────────────────────
    if cof is not None and cof > 0.015:
        pri = 2 if cof > 0.025 else 1
        items.append((pri,
            "With deposit costs having moved up, how are you making sure your "
            "asset yields are keeping pace — is spread management a key focus "
            "for the lending team right now?",
            f"Cost of funds is {cof:.2%}. Validates the pressure they're already "
            "feeling and opens discussion of yield improvement on the asset side."
        ))

    # ── HELOC utilization ────────────────────────────────────────────────────
    if "HELOC" in products:
        items.append((2,
            "On your home equity lines — what does typical member utilization "
            "look like? Do you find that a meaningful portion of committed capacity "
            "is sitting undrawn, or are members actively using those lines?",
            "Low HELOC utilization means committed capital isn't earning interest. "
            "This question surfaces the utilization gap naturally; Upstart HELOC's "
            "80% draw at origination converts committed capital to earning assets."
        ))

    # ── Unsecured NCO / credit quality ───────────────────────────────────────
    if "Unsecured Personal" in products and co is not None and co > 0.005:
        pri = 3 if co > 0.010 else 2
        items.append((pri,
            "In your unsecured consumer portfolio, how does the risk-adjusted "
            "return compare to your targets — are you seeing the credit quality "
            "on new originations that you'd expect when you underwrote those loans?",
            f"Portfolio NCO rate is {co:.2%}. Opens AI underwriting discussion "
            "without directly calling out loss rates. Leads to how Upstart's "
            "model improves selection quality in this segment."
        ))

    # ── Auto economics / dealer fees ────────────────────────────────────────
    if "Auto Retail" in products:
        items.append((2,
            "When you look at your indirect auto lending economics net of all "
            "origination costs — including dealer participation — how does the "
            "actual yield compare to your targets? Is there any channel mix "
            "adjustment you're considering?",
            "Dealer participation fees (~1%) and auto NCO compress net auto yield. "
            "This question surfaces the economics without criticising the dealer "
            "relationships. Leads to Upstart Auto Retail's no-fee, pre-underwritten model."
        ))

    # ── RE concentration ─────────────────────────────────────────────────────
    if "Unsecured Personal" in products or "Auto Retail" in products:
        items.append((1,
            "Your real estate portfolio is a strong foundation — as you think "
            "about the next growth phase, how are you approaching diversification "
            "across loan types? Are there consumer segments you'd like to grow into?",
            "Surfaces diversification ambition as a positive strategic goal, not "
            "a concern about RE concentration. Opens the door to consumer and "
            "auto loan growth where Upstart has the strongest product fit."
        ))

    # ── Wholesale / leverage-driven yield pressure ───────────────────────────
    if borr_pct is not None and borr_pct >= 0.03:
        pri = 2 if borr_pct >= 0.05 else 1
        items.append((pri,
            "As you've scaled to your current asset size, you've been strategic "
            "about your funding mix. How are you thinking about optimising asset "
            "yields to make sure the spread above your funding costs is where "
            "it needs to be for your earnings goals?",
            f"Wholesale borrowings are {borr_pct:.1%} of assets — elevated. "
            "Acknowledges their growth achievement before surfacing the yield "
            "pressure that wholesale funding creates."
        ))

    # ── Declining trend ──────────────────────────────────────────────────────
    trend_declining = any(
        "declining" in s.lower() or "below" in s.lower()
        for s in rec.get("signals", [])[:5]
    )
    if trend_declining:
        items.append((1,
            "Looking at your financial trends over the past few quarters, which "
            "metrics are you most focused on reversing or accelerating in your "
            "current strategic plan — and what's your biggest lever to move the needle?",
            "Opens a strategic planning conversation. If the exec surfaces the same "
            "metrics the data already identified, it confirms the pain and creates "
            "a natural segue to Upstart as a solution."
        ))

    # ── Capital & capacity ───────────────────────────────────────────────────
    if nwr is not None and nwr >= 0.10:
        items.append((1,
            "With a strong capital position, you clearly have the capacity to be "
            "aggressive on growth — what's holding you back from putting more of "
            "that capital to work in loans right now?",
            f"Net worth ratio is {nwr:.1%} — well-capitalised. Frames capital "
            "strength as an asset and asks what's constraining growth, opening "
            "the door to volume and origination capability discussion."
        ))

    # ── Universal discovery questions (always include, lowest priority) ──────
    items.append((0,
        "If you could change one thing about your current loan origination process "
        "— whether it's speed, scale, cost, or credit quality — what would it be?",
        "Universal discovery question. Surfaces the exec's own definition of the "
        "problem, which almost always maps to something Upstart addresses directly."
    ))
    items.append((0,
        "What does your loan growth pipeline look like for the next 12 months, "
        "and what's your biggest constraint on hitting those targets?",
        "Reveals whether the constraint is volume, margin, staffing, or credit "
        "quality — all of which Upstart can address. Good closing question if "
        "earlier questions didn't surface a clear pain point."
    ))

    # ── Sort: highest priority first, stable order within same priority ───────
    items.sort(key=lambda x: -x[0])

    # ── Build HTML ────────────────────────────────────────────────────────────
    CARD_BG   = "#fffdf4"
    HDR_CLR   = "#7d6608"
    BORD_CLR  = "#f0c040"

    def _badge(pri: int) -> str:
        if pri >= 3:
            return ('<span style="display:inline-block;font-size:.65rem;font-weight:700;'
                    'text-transform:uppercase;letter-spacing:.05em;background:#e8f8f0;'
                    'color:#1a6b3c;border:1px solid #a9dfbf;border-radius:3px;'
                    'padding:1px 6px;margin-right:8px;vertical-align:middle;">High</span>')
        if pri == 2:
            return ('<span style="display:inline-block;font-size:.65rem;font-weight:700;'
                    'text-transform:uppercase;letter-spacing:.05em;background:#fef9e7;'
                    'color:#9a7d0a;border:1px solid #f9e79f;border-radius:3px;'
                    'padding:1px 6px;margin-right:8px;vertical-align:middle;">Med</span>')
        if pri == 1:
            return ('<span style="display:inline-block;font-size:.65rem;font-weight:700;'
                    'text-transform:uppercase;letter-spacing:.05em;background:#eaf4fb;'
                    'color:#1a5276;border:1px solid #a9cce3;border-radius:3px;'
                    'padding:1px 6px;margin-right:8px;vertical-align:middle;">Low</span>')
        return ('<span style="display:inline-block;font-size:.65rem;font-weight:700;'
                'text-transform:uppercase;letter-spacing:.05em;background:#f4f6f9;'
                'color:#6c7a89;border:1px solid #ccd1d9;border-radius:3px;'
                'padding:1px 6px;margin-right:8px;vertical-align:middle;">Always</span>')

    rows_html = ""
    for i, (pri, question, note) in enumerate(items, 1):
        rows_html += f"""
    <div style="border-bottom:1px solid #f0e8c0;padding:14px 0;{'border-bottom:none;' if i == len(items) else ''}">
      <div style="display:flex;align-items:flex-start;gap:10px;">
        <span style="font-size:.78rem;font-weight:700;color:{HDR_CLR};min-width:22px;
                     padding-top:2px;">Q{i}</span>
        <div style="flex:1;">
          <p style="margin:0 0 6px;font-size:.92rem;font-weight:600;color:#2c3e50;
                    line-height:1.55;">
            {_badge(pri)}&ldquo;{question}&rdquo;
          </p>
          <p style="margin:0;font-size:.78rem;color:#7f8c8d;line-height:1.5;
                    font-style:italic;">
            <strong style="font-style:normal;color:#95a5a6;">Why it works:</strong>
            &nbsp;{note}
          </p>
        </div>
      </div>
    </div>"""

    return f"""
<!-- ── SALES CONVERSATION GUIDE (drew3.2) ──────────────────────────────── -->
<div class="card" id="sales-questions"
     style="background:{CARD_BG};border:1px solid {BORD_CLR};">
  <h2 style="color:{HDR_CLR};border-bottom-color:#f0e8c0;">
    Sales Conversation Guide &mdash; {cu_name}
    <span class="badge" style="background:#f0c040;color:#7d6608;">Internal Use Only</span>
  </h2>
  <p style="font-size:.86rem;color:#6c7a89;margin-bottom:18px;line-height:1.6;">
    Leading questions tailored to this credit union's financial profile.
    Ranked by relevance — start with the <strong>High</strong> items and work down.
    Each question is designed to surface the exec's own awareness of an opportunity
    without referencing metrics or implying a weakness in their balance sheet.
  </p>
  {rows_html}
  <p style="font-size:.75rem;color:#aab;margin-top:16px;font-style:italic;">
    Generated from NCUA 5300 call report data. For Upstart internal sales preparation only —
    not for distribution to credit union contacts.
  </p>
</div>
<!-- ── END SALES CONVERSATION GUIDE ──────────────────────────────────────── -->
"""


# ═══════════════════════════════════════════════════════════════════════════════
# END UPSTART RECOMMENDATION MODULE
# ═══════════════════════════════════════════════════════════════════════════════

# ─────────────────────────────────────────────────────────────────────────────
# HMDA / FFIEC constants
# ─────────────────────────────────────────────────────────────────────────────

HMDA_FFIEC_BASE = "https://ffiec.cfpb.gov/v2/data-browser-api/view/aggregations"
GLEIF_API       = "https://api.gleif.org/api/v1/lei-records"

HMDA_LOAN_TYPES: dict[str, str] = {
    "1": "Conventional",
    "2": "FHA",
    "3": "VA",
    "4": "USDA / Rural Housing",
}

HMDA_LOAN_PURPOSES: dict[str, str] = {
    "1":  "Home Purchase",
    "2":  "Home Improvement / HELOC",
    "31": "Refinancing",
    "32": "Cash-out Refinancing",
    "4":  "Other Purpose",
    "5":  "Not Applicable",
}


# ─────────────────────────────────────────────────────────────────────────────
# HMDA helpers
# ─────────────────────────────────────────────────────────────────────────────

def lookup_lei(cu_name: str) -> Optional[str]:
    """Look up a credit union's LEI from the GLEIF global registry."""
    name_lower = cu_name.lower()
    variants = [cu_name]
    if "credit union" not in name_lower and "cu" not in name_lower.split():
        variants.append(cu_name + " Credit Union")
        variants.append(cu_name + " Federal Credit Union")

    for variant in variants:
        try:
            resp = requests.get(
                GLEIF_API,
                params={"filter[entity.legalName]": variant, "page[size]": 5},
                timeout=15,
                headers={"User-Agent": "Mozilla/5.0 (NCUA-Dashboard/2.0)"},
            )
            if not resp.ok:
                continue
            for rec in resp.json().get("data", []):
                entity  = rec.get("attributes", {}).get("entity", {})
                country = entity.get("legalAddress", {}).get("country", "")
                legal   = entity.get("legalName", {}).get("name", "").lower()
                if country == "US" and ("credit union" in legal or variant.lower() in legal):
                    return rec["id"]
        except Exception:
            continue
    return None


HMDA_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}


def fetch_hmda_data(lei: str, cu_state: str = "") -> dict:
    """Fetch HMDA mortgage origination data from the FFIEC Data Browser API."""
    result: dict = {"lei": lei, "found": False}

    # Most recent published HMDA year (data lags ~9 months; cap at 2024)
    today     = date.today()
    hmda_year = today.year - 2 if today.month < 9 else today.year - 1
    hmda_year = min(hmda_year, 2024)
    result["year"] = hmda_year

    base_params = {"leis": lei, "years": str(hmda_year), "actions_taken": "1"}

    # Total originations
    try:
        resp = requests.get(HMDA_FFIEC_BASE, params=base_params,
                            headers=HMDA_HEADERS, timeout=15)
        if resp.ok:
            aggs = resp.json().get("aggregations", [])
            if aggs:
                result["total_count"] = aggs[0].get("count", 0)
                result["total_sum"]   = aggs[0].get("sum", 0.0)
                result["found"]       = result["total_count"] > 0
    except Exception:
        pass

    if not result["found"]:
        return result

    # By loan type
    try:
        resp = requests.get(
            HMDA_FFIEC_BASE,
            params={**base_params, "loan_types": "1,2,3,4"},
            headers=HMDA_HEADERS,
            timeout=15,
        )
        if resp.ok:
            result["by_loan_type"] = resp.json().get("aggregations", [])
    except Exception:
        pass

    # By loan purpose
    try:
        resp = requests.get(
            HMDA_FFIEC_BASE,
            params={**base_params, "loan_purposes": "1,2,31,32,4,5"},
            headers=HMDA_HEADERS,
            timeout=15,
        )
        if resp.ok:
            result["by_loan_purpose"] = resp.json().get("aggregations", [])
    except Exception:
        pass

    # Geographic: compare home-state count to total
    if cu_state:
        try:
            resp = requests.get(
                HMDA_FFIEC_BASE,
                params={**base_params, "states": cu_state},
                headers=HMDA_HEADERS,
                timeout=15,
            )
            if resp.ok:
                aggs = resp.json().get("aggregations", [])
                result["home_state"]       = cu_state
                result["home_state_count"] = aggs[0].get("count", 0) if aggs else 0
        except Exception:
            pass

    return result


def build_hmda_section(hmda: dict) -> str:
    """Build the HTML card for HMDA mortgage origination data."""
    if not hmda.get("found"):
        return ""

    year        = hmda.get("year", "")
    total_count = hmda.get("total_count", 0)
    total_sum   = hmda.get("total_sum", 0.0)
    DARK_HDR    = "#2c3e50"

    def _fmt_m(v: float) -> str:
        if v >= 1_000_000_000:
            return f"${v / 1_000_000_000:.2f}B"
        if v >= 1_000_000:
            return f"${v / 1_000_000:.1f}M"
        if v >= 1_000:
            return f"${v / 1_000:.0f}K"
        return f"${v:,.0f}"

    def _avg(count: int, total: float) -> str:
        return _fmt_m(total / count) if count else "—"

    def _tbl(title: str, rows_html: str, col1: str) -> str:
        return (
            f'<div style="flex:1;min-width:260px;">'
            f'<p style="font-size:.82rem;font-weight:600;color:{DARK_HDR};'
            f'text-transform:uppercase;letter-spacing:.04em;margin-bottom:8px;">{title}</p>'
            f'<table style="width:100%;border-collapse:collapse;font-size:.85rem;">'
            f'<thead><tr>'
            f'<th style="padding:7px 12px;background:{DARK_HDR};color:white;text-align:left;">{col1}</th>'
            f'<th style="padding:7px 12px;background:{DARK_HDR};color:white;text-align:right;">Count</th>'
            f'<th style="padding:7px 12px;background:{DARK_HDR};color:white;text-align:right;">Volume</th>'
            f'<th style="padding:7px 12px;background:{DARK_HDR};color:white;text-align:right;">Avg Loan</th>'
            f'</tr></thead>'
            f'<tbody>{rows_html}</tbody>'
            f'</table></div>'
        )

    # ── Loan Type table ──────────────────────────────────────────────────────
    lt_rows = ""
    for agg in sorted(hmda.get("by_loan_type", []), key=lambda x: x.get("count", 0), reverse=True):
        code  = str(agg.get("loan_types", ""))
        name  = HMDA_LOAN_TYPES.get(code, code)
        count = agg.get("count", 0)
        s     = agg.get("sum", 0.0)
        if not count:
            continue
        lt_rows += (
            f'<tr><td style="padding:7px 12px;">{name}</td>'
            f'<td style="padding:7px 12px;text-align:right;font-family:monospace;">{count:,}</td>'
            f'<td style="padding:7px 12px;text-align:right;font-family:monospace;">{_fmt_m(s)}</td>'
            f'<td style="padding:7px 12px;text-align:right;font-family:monospace;">{_avg(count, s)}</td>'
            f'</tr>\n'
        )
    if not lt_rows:
        lt_rows = '<tr><td colspan="4" style="padding:7px 12px;color:#999;">No data</td></tr>'

    # ── Loan Purpose table ───────────────────────────────────────────────────
    lp_rows = ""
    for agg in sorted(hmda.get("by_loan_purpose", []), key=lambda x: x.get("count", 0), reverse=True):
        code  = str(agg.get("loan_purposes", ""))
        name  = HMDA_LOAN_PURPOSES.get(code, code)
        count = agg.get("count", 0)
        s     = agg.get("sum", 0.0)
        if not count:
            continue
        lp_rows += (
            f'<tr><td style="padding:7px 12px;">{name}</td>'
            f'<td style="padding:7px 12px;text-align:right;font-family:monospace;">{count:,}</td>'
            f'<td style="padding:7px 12px;text-align:right;font-family:monospace;">{_fmt_m(s)}</td>'
            f'<td style="padding:7px 12px;text-align:right;font-family:monospace;">{_avg(count, s)}</td>'
            f'</tr>\n'
        )
    if not lp_rows:
        lp_rows = '<tr><td colspan="4" style="padding:7px 12px;color:#999;">No data</td></tr>'

    # ── Geographic note ──────────────────────────────────────────────────────
    geo_html = ""
    home_state = hmda.get("home_state", "")
    home_count = hmda.get("home_state_count", 0)
    if home_state and total_count > 0:
        home_pct = home_count / total_count * 100
        if home_pct >= 99.5:
            geo_note = f"100% of originations are in {home_state}."
        else:
            other_pct = 100 - home_pct
            geo_note = (
                f"{home_pct:.0f}% of originations are in {home_state}; "
                f"{other_pct:.0f}% are in other states."
            )
        geo_html = (
            f'<p style="margin-top:14px;font-size:.85rem;color:#555;">'
            f'&#128205; <strong>Geographic Distribution:</strong> {geo_note}</p>'
        )

    return (
        f'<div class="card">'
        f'<h2>HMDA Mortgage Originations <span class="badge">FFIEC · {year}</span></h2>'
        f'<div style="display:flex;align-items:center;gap:10px;margin-bottom:18px;'
        f'padding:10px 16px;background:#f0f7ff;border-radius:8px;border-left:3px solid #3498db;">'
        f'<span style="font-size:.84rem;color:#2980b9;line-height:1.5;">'
        f'<strong>{total_count:,}</strong> mortgage loans originated &nbsp;&middot;&nbsp; '
        f'<strong>{_fmt_m(total_sum)}</strong> total volume &nbsp;&middot;&nbsp; '
        f'Avg: <strong>{_avg(total_count, total_sum)}</strong>'
        f'<br><em style="opacity:.75;">HMDA covers mortgage-related products only '
        f'(home purchase, refi, home improvement, HELOC). '
        f'Auto, personal, and business loans are not included.</em>'
        f'</span></div>'
        f'<div style="display:flex;gap:32px;flex-wrap:wrap;">'
        f'{_tbl("By Loan Type", lt_rows, "Type")}'
        f'{_tbl("By Loan Purpose", lp_rows, "Purpose")}'
        f'</div>'
        f'{geo_html}'
        f'<p class="source-note" style="margin-top:12px;">'
        f'Source: <a href="https://ffiec.cfpb.gov/data-browser/" target="_blank">'
        f'FFIEC HMDA Data Browser</a> &nbsp;|&nbsp; '
        f'LEI: {hmda.get("lei", "")} &nbsp;|&nbsp; '
        f'Actions taken = 1 (originations only)'
        f'</p></div>'
    )


# ─────────────────────────────────────────────────────────────────────────────
# Quarter helpers
# ─────────────────────────────────────────────────────────────────────────────

def candidate_quarters(n: int = 6) -> list[tuple[int, int]]:
    """
    Return the last *n* quarter-end (year, month) tuples starting from the most
    recent quarter end that has already passed, newest-first. No publication-lag
    assumption — the caller probes each quarter with fetch_zip and stops once it
    has collected 3 that exist.
    """
    today = date.today()
    q_ends = [3, 6, 9, 12]
    available = [m for m in q_ends if m <= today.month]
    if available:
        last_m, last_y = max(available), today.year
    else:
        last_m, last_y = 12, today.year - 1

    out: list[tuple[int, int]] = []
    y, m = last_y, last_m
    for _ in range(n):
        out.append((y, m))
        m -= 3
        if m <= 0:
            m += 12
            y -= 1
    return out          # newest → oldest


def ql(year: int, month: int) -> str:
    return f"Q{month // 3} {year}"


def ann_factor(month: int) -> float:
    """Annualisation multiplier: YTD → full-year equivalent."""
    return {3: 4.0, 6: 2.0, 9: 4 / 3, 12: 1.0}[month]


# ─────────────────────────────────────────────────────────────────────────────
# Download & caching
# ─────────────────────────────────────────────────────────────────────────────

def _cache_path(year: int, month: int) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"call-report-data-{year}-{month:02d}.zip"


def fetch_zip(year: int, month: int) -> Optional[zipfile.ZipFile]:
    """Download (or load from cache) one quarter's NCUA bulk data zip."""
    cached = _cache_path(year, month)
    if cached.exists():
        print(f"    [cache] {ql(year, month)}")
        return zipfile.ZipFile(str(cached))

    for template in NCUA_ZIP_URLS:
        url = template.format(year=year, month=month)
        print(f"    Downloading {ql(year, month)}: {url}")
        try:
            r = requests.get(
                url, timeout=180, stream=True,
                headers={"User-Agent": "Mozilla/5.0 (NCUA-Dashboard/2.0)"},
            )
            r.raise_for_status()
            data = r.content
            cached.write_bytes(data)
            print(f"    Saved {len(data) / 1_048_576:.1f} MB → {cached.name}")
            return zipfile.ZipFile(io.BytesIO(data))
        except requests.HTTPError as e:
            print(f"    HTTP {e.response.status_code} – trying next URL")
        except Exception as e:
            print(f"    Error: {e}")

    print(f"    ✗ Could not download {ql(year, month)}")
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Zip file parsing
# ─────────────────────────────────────────────────────────────────────────────

def _read_zip_file(zf: zipfile.ZipFile, name: str) -> Optional[pd.DataFrame]:
    """Read a single file from a zip into a DataFrame with auto-detected settings."""
    try:
        raw = zf.read(name)
    except KeyError:
        return None

    text: Optional[str] = None
    for enc in ("utf-8", "latin-1", "cp1252"):
        try:
            text = raw.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    if text is None:
        return None

    # Auto-detect delimiter from first 2 KB
    sample = text[:2048]
    delim = max([",", "|", "\t"], key=lambda d: sample.count(d))

    try:
        df = pd.read_csv(
            io.StringIO(text), sep=delim, dtype=str,
            low_memory=False, keep_default_na=False,
        )
        # Normalise to UPPERCASE so mixed-case headers (e.g. "Acct_661A") match
        df.columns = [c.strip().upper() for c in df.columns]
        return df
    except Exception as e:
        print(f"    Could not parse {name}: {e}")
        return None


def _find_entry(entries: list[str], *keywords: str) -> Optional[str]:
    """Return the first zip entry whose name (uppercased) contains any keyword."""
    ku = [k.upper() for k in keywords]
    return next((e for e in entries if any(k in e.upper() for k in ku)), None)


def _col(df: pd.DataFrame, *candidates: str) -> Optional[str]:
    """Return the first DataFrame column matching any candidate (case-insensitive)."""
    col_upper = {c.upper(): c for c in df.columns}
    for cand in candidates:
        found = col_upper.get(cand.upper())
        if found is not None:
            return found
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Credit-union search
# ─────────────────────────────────────────────────────────────────────────────

def find_cu_in_zip(
    zf: zipfile.ZipFile,
    name_query: str,
    charter: Optional[str] = None,
) -> Optional[dict]:
    """
    Locate a credit union in the FOICU identification file inside the zip.
    Returns {"cu_number": str, "cu_name": str} or None.
    Prompts the user to choose if multiple matches are found.
    """
    entries = zf.namelist()

    # FOICU.txt is the primary identification file; fall back to any txt/csv
    info_file = (
        _find_entry(entries, "FOICU")
        or _find_entry(entries, "CU_INFO", "CREDIT_UNION")
        or next(
            (e for e in entries
             if e.upper().endswith((".TXT", ".CSV"))
             and "ACCT" not in e.upper() and "FS220" not in e.upper()),
            None,
        )
    )
    if info_file is None:
        print("    ✗ No identification file found in zip.")
        return None

    df = _read_zip_file(zf, info_file)
    if df is None:
        return None

    name_col = _col(df, "CU_NAME", "CREDIT_UNION_NAME", "NAME", "CU NAME")
    num_col  = _col(df, "CU_NUMBER", "CHARTER_NUMBER", "CU_NUM", "CHARTER", "CU NUMBER")

    if name_col is None or num_col is None:
        print(f"    ✗ Could not find name/charter columns in {info_file}.")
        print(f"      Available columns: {list(df.columns[:15])}")
        return None

    if charter:
        hits = df[df[num_col].str.strip() == charter.strip()]
    else:
        # Strip generic words that appear in almost every CU name so they
        # don't inflate scores for unrelated credit unions.
        STOP = {
            "FEDERAL", "CREDIT", "UNION", "THE", "AND", "FOR", "INC",
            "INCORPORATED", "LLC", "LTD", "CORP",
        }
        q_words = [
            w for w in name_query.upper().split()
            if len(w) >= 3 and w not in STOP
        ]
        # Fall back to all words (including generic) if no significant words
        if not q_words:
            q_words = [w for w in name_query.upper().split() if len(w) >= 3]

        def word_overlap(stored_name: str) -> float:
            """Jaccard-style score: overlap / union of significant words.
            Penalizes stored names with many extra words, so 'AMERICA FIRST'
            ranks above 'FIRST CHOICE AMERICA COMMUNITY' for query 'america first'."""
            s_words = [
                w for w in stored_name.upper().split()
                if len(w) >= 3 and w not in STOP
            ]
            if not s_words:
                return 0.0
            hits_count = sum(1 for w in q_words if w in stored_name.upper())
            union = len(set(q_words) | set(s_words))
            return hits_count / union

        scores = df[name_col].apply(word_overlap)
        hits = df[scores > 0].copy()
        if not hits.empty:
            hits = hits.assign(_score=scores[hits.index]).sort_values(
                "_score", ascending=False
            ).drop(columns="_score")

    if hits.empty:
        return None

    if len(hits) > 1:
        print(f"\n  {len(hits)} credit unions matched '{name_query}':")
        display = hits.head(20)
        for i, (_, row) in enumerate(display.iterrows()):
            print(
                f"    [{i + 1:>2}]  {str(row[name_col]).strip():<50}  "
                f"Charter #{str(row[num_col]).strip()}"
            )
        if len(hits) > 20:
            print(f"         … and {len(hits) - 20} more. Refine your search if needed.")

        # Auto-select top match when not running interactively (e.g. piped input)
        if not sys.stdin.isatty():
            print("  (Non-interactive mode — auto-selecting best match [1])")
            hits = hits.iloc[[0]]
        else:
            while True:
                sel = input("  Select number [1]: ").strip() or "1"
                try:
                    idx = int(sel) - 1
                    if 0 <= idx < len(display):
                        hits = hits.iloc[[idx]]
                        break
                except ValueError:
                    pass
                print("  Invalid – enter a number from the list.")

    row = hits.iloc[0]

    def foicu(*fields: str) -> str:
        col = _col(df, *fields)
        return str(row[col]).strip() if col and col in row.index else ""

    tom   = foicu("TOM_CODE").lstrip("0") or "0"
    tom_k = foicu("TOM_CODE").strip().zfill(2)
    fom   = TOM_DESC.get(tom_k, TOM_DESC.get(tom.zfill(2), "Occupational / Associational"))
    ctype = CU_TYPE_DESC.get(foicu("CU_TYPE"), "Credit Union")

    # CEO data lives in FS220D.txt (columns: CEO_F = first, CEO = last)
    ceo_name: Optional[str] = None
    fs220d_entry = _find_entry(entries, "FS220D")
    if fs220d_entry:
        df_d = _read_zip_file(zf, fs220d_entry)
        if df_d is not None:
            cu_num = str(row[num_col]).strip()
            num_col_d = _col(df_d, "CU_NUMBER", "CHARTER_NUMBER", "CU_NUM")
            if num_col_d:
                mask_d = df_d[num_col_d].str.strip().str.lstrip("0") == cu_num.lstrip("0")
                hits_d = df_d[mask_d]
                if not hits_d.empty:
                    d_row = hits_d.iloc[0]
                    first_col = _col(df_d, "CEO_F")
                    last_col  = _col(df_d, "CEO")
                    ceo_first = str(d_row[first_col]).strip().title() if first_col else ""
                    ceo_last  = str(d_row[last_col]).strip().title() if last_col else ""
                    ceo_name  = f"{ceo_first} {ceo_last}".strip() or None

    # Branch count is derived from "Credit Union Branch Information.txt" (one row per branch)
    num_branches: Optional[int] = None
    branch_entry = _find_entry(entries, "BRANCH")
    if branch_entry:
        df_b = _read_zip_file(zf, branch_entry)
        if df_b is not None:
            cu_num = str(row[num_col]).strip()
            num_col_b = _col(df_b, "CU_NUMBER", "CHARTER_NUMBER", "CU_NUM")
            if num_col_b:
                mask_b = df_b[num_col_b].astype(str).str.strip().str.lstrip("0") == cu_num.lstrip("0")
                num_branches = int(mask_b.sum()) or None

    return {
        "cu_number":    str(row[num_col]).strip(),
        "cu_name":      str(row[name_col]).strip(),
        "city":         foicu("CITY").title(),
        "state":        foicu("STATE"),
        "cu_type":      ctype,
        "fom":          fom,
        "year_opened":  foicu("YEAR_OPENED"),
        "peer_group":   foicu("PEER_GROUP"),
        "low_income":   foicu("LIMITED_INC") == "1",
        "ceo_name":     ceo_name,
        "num_branches": num_branches,
    }


# ─────────────────────────────────────────────────────────────────────────────
# 5300 financial data extraction
# ─────────────────────────────────────────────────────────────────────────────

def _resolve_charter(zf: zipfile.ZipFile, cu_name: str) -> Optional[str]:
    """Return the charter number for *cu_name* (exact, case-insensitive) in this zip's FOICU."""
    entries = zf.namelist()
    info_file = _find_entry(entries, "FOICU")
    if info_file is None:
        return None
    df = _read_zip_file(zf, info_file)
    if df is None:
        return None
    name_col = _col(df, "CU_NAME", "CREDIT_UNION_NAME", "NAME", "CU NAME")
    num_col  = _col(df, "CU_NUMBER", "CHARTER_NUMBER", "CU_NUM", "CHARTER", "CU NUMBER")
    if name_col is None or num_col is None:
        return None
    mask = df[name_col].str.strip().str.upper() == cu_name.strip().upper()
    hits = df[mask]
    if hits.empty:
        return None
    return str(hits.iloc[0][num_col]).strip()


def _get_cu_row(df: pd.DataFrame, cu_number: str) -> Optional[pd.Series]:
    """Return the row for *cu_number* from *df*, tolerating leading-zero differences."""
    num_col = _col(df, "CU_NUMBER", "CHARTER_NUMBER", "CU_NUM", "CHARTER", "CU NUMBER")
    if num_col is None:
        return None
    target = cu_number.lstrip("0")
    mask = df[num_col].str.strip().str.lstrip("0") == target
    hits = df[mask]
    if hits.empty:
        hits = df[df[num_col].str.strip() == cu_number]
    return hits.iloc[0] if not hits.empty else None


def extract_financials(
    zf: zipfile.ZipFile,
    cu_number: str,
    year: int,
    month: int,
) -> Optional[dict]:
    """
    Pull a credit union's financial data by merging FS220.txt (main) and
    FS220A.txt (supplemental, contains net worth and net income).
    Returns a flat dict of UPPERCASE_column → value plus _year/_month.
    """
    entries = zf.namelist()

    def read_cu(filename: str) -> Optional[pd.Series]:
        df = _read_zip_file(zf, filename)
        if df is None:
            return None
        return _get_cu_row(df, cu_number)

    # Primary file
    fs220_name = next((e for e in entries if e.upper() == "FS220.TXT"), None)
    if fs220_name is None:
        print(f"    ✗ FS220.txt not found in {ql(year, month)} zip.")
        return None

    row_main = read_cu(fs220_name)
    if row_main is None:
        print(f"    ✗ Charter #{cu_number} not found in FS220.txt ({ql(year, month)}).")
        return None

    record = row_main.to_dict()

    # Supplemental files — merge without overwriting shared identifier columns
    for supp in ["FS220A.TXT", "FS220B.TXT", "FS220C.TXT", "FS220H.TXT", "FS220I.TXT", "FS220L.TXT", "FS220M.TXT", "FS220P.TXT", "FS220Q.TXT"]:
        supp_name = next((e for e in entries if e.upper() == supp), None)
        if supp_name:
            row_s = read_cu(supp_name)
            if row_s is not None:
                for k, v in row_s.items():
                    if k not in record:
                        record[k] = v

    record["_year"]  = year
    record["_month"] = month
    return record


# ─────────────────────────────────────────────────────────────────────────────
# Shares & Deposits extraction and table builder
# ─────────────────────────────────────────────────────────────────────────────

SHARE_CATEGORIES: list[tuple[str, str]] = [
    ("share_drafts",        "Share Drafts"),
    ("regular_shares",      "Regular Shares"),
    ("money_market_shares", "Money Market Shares"),
    ("share_certificates",  "Share Certificates"),
    ("ira_keogh",           "IRA/KEOGH Accounts"),
    ("other_shares",        "All Other Shares"),
    ("non_member_deposits",  "Non-Member Deposits"),
    ("total",                "TOTAL SHARES & DEPOSITS"),
    ("total_borrowings",     "Total Borrowings"),
]


def extract_shares(record: dict) -> dict:
    """Pull share-category dollar amounts from a raw financial record."""
    vals: dict = {}
    for key in ("share_drafts", "regular_shares", "money_market_shares",
                "share_certificates", "ira_keogh",
                "total_shares_no_nm", "non_member_deposits", "total_shares",
                "total_borrowings"):
        vals[key] = _get(record, key)

    # All Other Shares = total (excl. non-member) − known categories
    no_nm = vals.get("total_shares_no_nm")
    known = sum(
        vals.get(k) or 0.0
        for k in ("share_drafts", "regular_shares", "money_market_shares",
                  "share_certificates", "ira_keogh")
    )
    vals["other_shares"] = (no_nm - known) if no_nm is not None else None
    vals["total"]        = vals.get("total_shares")   # ACCT_018
    return vals


def _fmt_dollars(v: Optional[float]) -> str:
    if v is None:
        return "—"
    return f"${v:,.0f}"


def _fmt_change(current: Optional[float], base: Optional[float]) -> tuple[str, str]:
    """Return (formatted_pct, css_color) for a period-over-period change."""
    if current is None or base is None or base == 0:
        return "#DIV/0!", "#999"
    pct = (current - base) / base
    arrow = "▲" if pct >= 0 else "▼"
    color = "#27ae60" if pct >= 0 else "#e74c3c"
    return f'{arrow} {pct:+.2%}', color


def build_shares_table(
    ya_label: str,   prior_label: str,   cur_label: str,
    ya_shares: dict, prior_shares: dict, cur_shares: dict,
) -> str:
    """Build the Shares & Deposits HTML card."""
    DARK_HDR = "#2c3e50"

    header = (
        f'<thead><tr>'
        f'<th style="padding:9px 14px;background:{DARK_HDR};color:white;">Category</th>'
        f'<th style="padding:9px 14px;background:{DARK_HDR};color:white;text-align:right;">{ya_label}</th>'
        f'<th style="padding:9px 14px;background:{DARK_HDR};color:white;text-align:right;">{prior_label}</th>'
        f'<th style="padding:9px 14px;background:{DARK_HDR};color:white;text-align:right;">{cur_label}</th>'
        f'<th style="padding:9px 14px;background:{DARK_HDR};color:white;text-align:center;">QoQ Change</th>'
        f'<th style="padding:9px 14px;background:{DARK_HDR};color:white;text-align:center;">YoY Change</th>'
        f'</tr></thead>'
    )

    rows_html = ""
    for key, label in SHARE_CATEGORIES:
        is_total      = key == "total"
        is_borrowings = key == "total_borrowings"
        weight   = "700" if (is_total or is_borrowings) else "400"
        bg       = "#f0f4f8" if is_total else ("#fef9f0" if is_borrowings else "white")
        border   = "border-top:2px solid #dee2e6;" if is_total else (
                   "border-top:1px solid #dee2e6;" if is_borrowings else "")

        ya_v    = ya_shares.get(key)
        prior_v = prior_shares.get(key)
        cur_v   = cur_shares.get(key)

        qoq_txt, qoq_col = _fmt_change(cur_v, prior_v)
        yoy_txt, yoy_col = _fmt_change(cur_v, ya_v)

        rows_html += (
            f'<tr style="background:{bg};{border}">'
            f'<td style="padding:8px 14px;font-weight:{weight};">{label}</td>'
            f'<td style="padding:8px 14px;text-align:right;font-family:monospace;">{_fmt_dollars(ya_v)}</td>'
            f'<td style="padding:8px 14px;text-align:right;font-family:monospace;">{_fmt_dollars(prior_v)}</td>'
            f'<td style="padding:8px 14px;text-align:right;font-family:monospace;font-weight:{weight};">{_fmt_dollars(cur_v)}</td>'
            f'<td style="padding:8px 14px;text-align:center;color:{qoq_col};font-weight:600;">{qoq_txt}</td>'
            f'<td style="padding:8px 14px;text-align:center;color:{yoy_col};font-weight:600;">{yoy_txt}</td>'
            f'</tr>\n'
        )

    return (
        f'<div class="card">'
        f'<h2>Shares &amp; Deposits Breakdown '
        f'<span class="badge">{ya_label} · {prior_label} · {cur_label}</span></h2>'
        f'<div style="overflow-x:auto;">'
        f'<table>{header}<tbody>{rows_html}</tbody></table>'
        f'</div>'
        f'<p class="source-note" style="margin-top:10px;">'
        f'Source: NCUA 5300 FS220.txt &nbsp;|&nbsp; '
        f'QoQ = quarter-over-quarter vs {prior_label} &nbsp;|&nbsp; '
        f'YoY = year-over-year vs {ya_label}.'
        f'</p></div>'
    )


INVEST_TYPE_CATEGORIES: list[tuple[str, str]] = [
    ("invest_cash_deposits", "Cash & Other Deposits"),
    ("invest_securities",    "Investment Securities"),
    ("invest_other",         "Other Investments"),
    ("total_invest",         "TOTAL INVESTMENT PORTFOLIO"),
]

INVEST_MATURITY_BUCKETS: list[tuple[str, str]] = [
    ("invest_short_term", "< 1 Year"),
    ("invest_1_3yr",      "1 – 3 Years"),
    ("invest_3_5yr",      "3 – 5 Years"),
    ("invest_5_10yr",     "5 – 10 Years"),
    ("invest_10yr_plus",  "> 10 Years"),
]


def extract_investments(record: dict) -> dict:
    """Pull investment type and maturity dollar amounts from a raw financial record."""
    vals: dict = {}
    for key in ("invest_cash_deposits", "invest_securities", "invest_other",
                "invest_short_term", "invest_1_3yr", "invest_3_5yr",
                "invest_5_10yr", "invest_10yr_plus"):
        vals[key] = _get(record, key)
    cash = vals.get("invest_cash_deposits")
    sec  = vals.get("invest_securities")
    oth  = vals.get("invest_other")
    total = (cash or 0.0) + (sec or 0.0) + (oth or 0.0)
    vals["total_invest"] = total if total > 0 else None
    return vals


def compute_investment_yield(record: dict) -> Optional[float]:
    """Annualised investment income / total investment portfolio."""
    income = _get(record, "invest_income_ytd")
    cash   = _get(record, "invest_cash_deposits")
    sec    = _get(record, "invest_securities")
    oth    = _get(record, "invest_other")
    total  = (cash or 0.0) + (sec or 0.0) + (oth or 0.0)
    if income is None or total == 0:
        return None
    return (income * ann_factor(record.get("_month", 12))) / total


def build_investments_table(
    ya_label: str,  prior_label: str,  cur_label: str,
    ya_inv: dict,   prior_inv: dict,   cur_inv: dict,
    cur_yield: Optional[float] = None,
) -> str:
    """Build the Investment Portfolio HTML card with type and maturity breakdowns."""
    DARK_HDR   = "#2c3e50"
    INV_HDR    = "#1a5276"   # accent for the yield column
    SEC_STYLE  = (
        "font-size:.78rem;font-weight:700;text-transform:uppercase;"
        "letter-spacing:.06em;color:#6c7a89;padding:10px 14px 4px;border-bottom:none;"
    )

    # ── By Investment Type table ─────────────────────────────────────────────
    type_hdr = (
        f'<thead><tr>'
        f'<th style="padding:9px 14px;background:{DARK_HDR};color:white;">Type</th>'
        f'<th style="padding:9px 14px;background:{DARK_HDR};color:white;text-align:right;">{ya_label}</th>'
        f'<th style="padding:9px 14px;background:{DARK_HDR};color:white;text-align:right;">{prior_label}</th>'
        f'<th style="padding:9px 14px;background:{DARK_HDR};color:white;text-align:right;">{cur_label}</th>'
        f'<th style="padding:9px 14px;background:{DARK_HDR};color:white;text-align:center;">QoQ</th>'
        f'<th style="padding:9px 14px;background:{DARK_HDR};color:white;text-align:center;">YoY</th>'
        f'</tr></thead>'
    )
    type_rows = ""
    for key, label in INVEST_TYPE_CATEGORIES:
        is_total = key == "total_invest"
        weight   = "700" if is_total else "400"
        bg       = "#f0f4f8" if is_total else "white"
        border   = "border-top:2px solid #dee2e6;" if is_total else ""
        ya_v     = ya_inv.get(key)
        prior_v  = prior_inv.get(key)
        cur_v    = cur_inv.get(key)
        qoq_txt, qoq_col = _fmt_change(cur_v, prior_v)
        yoy_txt, yoy_col = _fmt_change(cur_v, ya_v)
        type_rows += (
            f'<tr style="background:{bg};{border}">'
            f'<td style="padding:8px 14px;font-weight:{weight};">{label}</td>'
            f'<td style="padding:8px 14px;text-align:right;font-family:monospace;">{_fmt_dollars(ya_v)}</td>'
            f'<td style="padding:8px 14px;text-align:right;font-family:monospace;">{_fmt_dollars(prior_v)}</td>'
            f'<td style="padding:8px 14px;text-align:right;font-family:monospace;font-weight:{weight};">{_fmt_dollars(cur_v)}</td>'
            f'<td style="padding:8px 14px;text-align:center;color:{qoq_col};font-weight:600;">{qoq_txt}</td>'
            f'<td style="padding:8px 14px;text-align:center;color:{yoy_col};font-weight:600;">{yoy_txt}</td>'
            f'</tr>\n'
        )

    # ── By Maturity table (3-period; % of cur total) ─────────────────────────
    cur_total  = cur_inv.get("total_invest") or 0.0
    prior_total = prior_inv.get("total_invest") or 0.0
    ya_total   = ya_inv.get("total_invest") or 0.0

    mat_hdr = (
        f'<thead><tr>'
        f'<th style="padding:9px 14px;background:{INV_HDR};color:white;">Maturity Bucket</th>'
        f'<th style="padding:9px 14px;background:{INV_HDR};color:white;text-align:right;">{ya_label}</th>'
        f'<th style="padding:9px 14px;background:{INV_HDR};color:white;text-align:right;">{prior_label}</th>'
        f'<th style="padding:9px 14px;background:{INV_HDR};color:white;text-align:right;">{cur_label}</th>'
        f'<th style="padding:9px 14px;background:{INV_HDR};color:white;text-align:center;">% of Portfolio</th>'
        f'</tr></thead>'
    )
    mat_rows = ""
    any_maturity = any(
        cur_inv.get(k) is not None
        for k, _ in INVEST_MATURITY_BUCKETS
    )
    if any_maturity:
        for key, label in INVEST_MATURITY_BUCKETS:
            ya_v    = ya_inv.get(key)
            prior_v = prior_inv.get(key)
            cur_v   = cur_inv.get(key)
            pct_str = f"{cur_v / cur_total:.1%}" if cur_v is not None and cur_total > 0 else "—"
            mat_rows += (
                f'<tr style="background:white;">'
                f'<td style="padding:8px 14px;">{label}</td>'
                f'<td style="padding:8px 14px;text-align:right;font-family:monospace;">{_fmt_dollars(ya_v)}</td>'
                f'<td style="padding:8px 14px;text-align:right;font-family:monospace;">{_fmt_dollars(prior_v)}</td>'
                f'<td style="padding:8px 14px;text-align:right;font-family:monospace;font-weight:600;">{_fmt_dollars(cur_v)}</td>'
                f'<td style="padding:8px 14px;text-align:center;font-family:monospace;">{pct_str}</td>'
                f'</tr>\n'
            )
    else:
        mat_rows = (
            f'<tr><td colspan="5" style="padding:14px;text-align:center;'
            f'color:#95a5a6;font-style:italic;">Maturity schedule not reported '
            f'(FS220Q not available for this credit union)</td></tr>'
        )

    yield_str = f"{cur_yield:.2%}" if cur_yield else "—"

    return (
        f'<div class="card">'
        f'<h2>Investment Portfolio Breakdown '
        f'<span class="badge">{ya_label} · {prior_label} · {cur_label}</span>'
        f'&nbsp;<span style="margin-left:auto;font-size:.82rem;font-weight:500;'
        f'color:{INV_HDR};">Portfolio Yield: <strong>{yield_str}</strong></span></h2>'
        f'<div style="overflow-x:auto;">'
        f'<table style="margin-bottom:0;">'
        f'<tbody><tr><td colspan="6" style="{SEC_STYLE}">By Investment Type</td></tr></tbody>'
        f'{type_hdr}<tbody>{type_rows}</tbody>'
        f'</table>'
        f'<table style="margin-top:18px;">'
        f'<tbody><tr><td colspan="5" style="{SEC_STYLE}">By Maturity Bucket</td></tr></tbody>'
        f'{mat_hdr}<tbody>{mat_rows}</tbody>'
        f'</table>'
        f'</div>'
        f'<p class="source-note" style="margin-top:10px;">'
        f'Source: NCUA 5300 FS220P (investment types) · FS220Q (maturity schedule) · FS220A (income) '
        f'&nbsp;|&nbsp; QoQ = quarter-over-quarter vs {prior_label} '
        f'&nbsp;|&nbsp; YoY = year-over-year vs {ya_label} '
        f'&nbsp;|&nbsp; Portfolio Yield = annualised investment income ÷ total portfolio'
        f'</p></div>'
    )


ASSET_CLASSES: list[tuple[str, list[str]]] = [
    ("Unsecured",   ["loan_credit_card", "loan_pal", "loan_student", "loan_other_unsecured"]),
    ("Auto",        ["loan_new_vehicle", "loan_used_vehicle", "loan_leases"]),
    ("Residential", ["loan_re_1st_lien", "loan_re_junior_lien", "loan_re_other"]),
    ("Commercial",  ["loan_commercial_re", "loan_commercial_nonre"]),
    ("Other",       ["loan_other_secured"]),
]

ASSET_CLASS_COLORS = ["#3498db", "#e67e22", "#2ecc71", "#9b59b6", "#95a5a6"]

LOAN_CATEGORIES: list[tuple[str, str]] = [
    ("loan_credit_card",     "Unsecured Credit Card Loans"),
    ("loan_pal",             "Payday Alternative Loans (PAL I & II — FCUs only)"),
    ("loan_student",         "Non-Federally Guaranteed Student Loans"),
    ("loan_other_unsecured", "All Other Unsecured Loans/Lines of Credit"),
    ("loan_new_vehicle",     "New Vehicle Loans"),
    ("loan_used_vehicle",    "Used Vehicle Loans"),
    ("loan_leases",          "Leases Receivable"),
    ("loan_other_secured",   "All Other Secured Non-Real Estate Loans/Lines of Credit"),
    ("loan_re_1st_lien",     "1- to 4-Family Residential — 1st Lien"),
    ("loan_re_junior_lien",  "1- to 4-Family Residential — Junior Lien"),
    ("unfunded_re_junior_lien", "Unfunded Commitments"),
    ("loan_re_other",        "All Other (Non-Commercial) Real Estate Loans/Lines of Credit"),
    ("loan_commercial_re",   "Commercial Loans/Lines of Credit — Real Estate Secured"),
    ("loan_commercial_nonre","Commercial Loans/Lines of Credit — Not Real Estate Secured"),
    ("total_loans",          "TOTAL LOANS & LEASES"),
]


LOAN_RATE_KEY: dict[str, str] = {
    "loan_credit_card":      "rate_credit_card",
    "loan_pal":              "rate_pal",
    "loan_student":          "rate_student",
    "loan_other_unsecured":  "rate_other_unsecured",
    "loan_new_vehicle":      "rate_new_auto",
    "loan_used_vehicle":     "rate_used_auto",
    "loan_leases":           "rate_leases",
    "loan_other_secured":    "rate_other_secured",
    "loan_re_1st_lien":      "rate_re_1st_lien",
    "loan_re_junior_lien":   "rate_re_junior_lien",
    "loan_re_other":         "rate_re_other",
    "loan_commercial_re":    "rate_commercial_re",
    "loan_commercial_nonre": "rate_commercial_nonre",
}

# Maps loan_key → ([chargeoff_acct_keys], [recovery_acct_keys])
# Commercial RE/non-RE use multiple sub-keys that are summed in extract_loan_losses().
LOAN_LOSS_KEY: dict[str, tuple[list, list]] = {
    "loan_credit_card":      (["co_credit_card"],    ["rec_credit_card"]),
    "loan_pal":              (["co_pal"],             ["rec_pal"]),
    "loan_student":          (["co_student"],         ["rec_student"]),
    "loan_other_unsecured":  (["co_other_unsecured"], ["rec_other_unsecured"]),
    "loan_new_vehicle":      (["co_new_vehicle"],     ["rec_new_vehicle"]),
    "loan_used_vehicle":     (["co_used_vehicle"],    ["rec_used_vehicle"]),
    "loan_leases":           (["co_leases"],          ["rec_leases"]),
    "loan_other_secured":    (["co_other_secured"],   ["rec_other_secured"]),
    "loan_re_1st_lien":      (["co_re_1st_lien"],     ["rec_re_1st_lien"]),
    "loan_re_junior_lien":   (["co_re_junior_lien"],  ["rec_re_junior_lien"]),
    "loan_re_other":         (["co_re_other"],        ["rec_re_other"]),
    "loan_commercial_re":    (
        ["co_comm_re_constr", "co_comm_re_farm", "co_comm_re_multi",
         "co_comm_re_owner",  "co_comm_re_nonown"],
        ["rec_comm_re_constr","rec_comm_re_farm","rec_comm_re_multi",
         "rec_comm_re_owner", "rec_comm_re_nonown"],
    ),
    "loan_commercial_nonre": (
        ["co_comm_nonre_ag",   "co_comm_nonre_ci",
         "co_comm_nonre_unsec","co_comm_nonre_rev"],
        ["rec_comm_nonre_ag",  "rec_comm_nonre_ci",
         "rec_comm_nonre_unsec","rec_comm_nonre_rev"],
    ),
}


def extract_loans(record: dict) -> dict:
    """Pull loan-category dollar amounts from a raw financial record."""
    vals: dict = {}
    for key, _ in LOAN_CATEGORIES:
        vals[key] = _get(record, key)
    return vals


def extract_loan_rates(record: dict) -> dict:
    """Extract per-product loan rates (basis points → %) from a financial record.
    Returns {rate_key: float} where float is the rate as a decimal (e.g. 0.06 = 6%).
    0 values from NCUA indicate 'not reported' and are returned as None."""
    rates: dict = {}
    for rate_key in set(LOAN_RATE_KEY.values()):
        bp = _get(record, rate_key)
        rates[rate_key] = (bp / 100.0 / 100.0) if bp and bp > 0 else None
    return rates


def extract_loan_losses(record: dict) -> dict:
    """Compute annualized net charge-offs per loan category.
    Returns {loan_key: annualized_net_co_dollars} or None where not reported."""
    af = ann_factor(record.get("_month", 12))
    results: dict = {}
    for loan_key, (co_keys, rec_keys) in LOAN_LOSS_KEY.items():
        co  = sum(_get(record, k) or 0 for k in co_keys)
        rec = sum(_get(record, k) or 0 for k in rec_keys)
        results[loan_key] = (co - rec) * af if (co or rec) else None
    return results


def compute_portfolio_yield(record: dict) -> Optional[float]:
    """Compute overall loan yield = annualized interest income / total loans."""
    income = _get(record, "interest_income_ytd")
    loans  = _get(record, "total_loans")
    if income is None or loans is None or loans == 0:
        return None
    return (income * ann_factor(record.get("_month", 12))) / loans


def compute_portfolio_nco(record: dict) -> Optional[float]:
    """Compute portfolio-level annualized net charge-off rate = (gross C/O - recoveries) / total loans."""
    co   = _get(record, "gross_chargeoffs_ytd")
    rec  = _get(record, "recoveries_ytd")
    loans = _get(record, "total_loans")
    if not loans:
        return None
    net = ((co or 0) - (rec or 0)) * ann_factor(record.get("_month", 12))
    return net / loans


def build_loans_table(
    ya_label: str,  prior_label: str,  cur_label: str,
    ya_loans: dict, prior_loans: dict, cur_loans: dict,
    cur_rates: Optional[dict] = None,
    cur_portfolio_yield: Optional[float] = None,
    cur_losses: Optional[dict] = None,
    cur_portfolio_nco: Optional[float] = None,
) -> str:
    """Build the Loans breakdown HTML card."""
    DARK_HDR  = "#2c3e50"
    LOSS_HDR  = "#7b241c"

    show_rate = cur_rates is not None
    show_loss = cur_losses is not None

    header = (
        f'<thead><tr>'
        f'<th style="padding:9px 14px;background:{DARK_HDR};color:white;">Category</th>'
        f'<th style="padding:9px 14px;background:{DARK_HDR};color:white;text-align:right;">{ya_label}</th>'
        f'<th style="padding:9px 14px;background:{DARK_HDR};color:white;text-align:right;">{prior_label}</th>'
        f'<th style="padding:9px 14px;background:{DARK_HDR};color:white;text-align:right;">{cur_label}</th>'
        + (f'<th style="padding:9px 14px;background:#1a5276;color:white;text-align:center;">Loan Rate</th>' if show_rate else '')
        + (f'<th style="padding:9px 14px;background:{LOSS_HDR};color:white;text-align:center;">Annl. NCO%</th>' if show_loss else '')
        + f'<th style="padding:9px 14px;background:{DARK_HDR};color:white;text-align:center;">QoQ Change</th>'
        f'<th style="padding:9px 14px;background:{DARK_HDR};color:white;text-align:center;">YoY Change</th>'
        f'</tr></thead>'
    )

    rows_html = ""
    for key, label in LOAN_CATEGORIES:
        is_total    = key == "total_loans"
        is_unfunded = key == "unfunded_re_junior_lien"
        weight   = "700" if is_total else "400"
        bg       = "#f0f4f8" if is_total else ("#fafbfc" if is_unfunded else "white")
        border   = "border-top:2px solid #dee2e6;" if is_total else ""

        ya_v    = ya_loans.get(key)
        prior_v = prior_loans.get(key)
        cur_v   = cur_loans.get(key)

        qoq_txt, qoq_col = _fmt_change(cur_v, prior_v)
        yoy_txt, yoy_col = _fmt_change(cur_v, ya_v)

        # Rate cell
        rate_td = ""
        if show_rate:
            if is_unfunded:
                funded   = cur_loans.get("loan_re_junior_lien")
                unfunded = cur_v
                if funded and unfunded is not None and (funded + unfunded) > 0:
                    util = funded / (funded + unfunded)
                    rate_str = f"Util: {util:.1%}"
                else:
                    rate_str = "—"
                rate_td = (
                    f'<td style="padding:8px 14px;text-align:center;'
                    f'font-family:monospace;color:#555;font-style:italic;">'
                    f'{rate_str}</td>'
                )
            elif is_total:
                pct = cur_portfolio_yield
                rate_str = f"{pct:.2%}" if pct else "—"
                rate_td = (
                    f'<td style="padding:8px 14px;text-align:center;'
                    f'font-family:monospace;color:#1a5276;font-weight:600;">'
                    f'{rate_str}</td>'
                )
            else:
                rk  = LOAN_RATE_KEY.get(key)
                pct = cur_rates.get(rk) if rk else None
                rate_str = f"{pct:.2%}" if pct else "—"
                rate_td = (
                    f'<td style="padding:8px 14px;text-align:center;'
                    f'font-family:monospace;color:#1a5276;font-weight:600;">'
                    f'{rate_str}</td>'
                )

        # Loss (NCO) cell
        loss_td = ""
        if show_loss:
            if is_unfunded:
                loss_td = f'<td style="padding:8px 14px;"></td>'
            elif is_total:
                nco_str = f"{cur_portfolio_nco:.2%}" if cur_portfolio_nco is not None else "—"
                nco_color = "#922b21" if (cur_portfolio_nco or 0) > 0 else "#1e8449"
                loss_td = (
                    f'<td style="padding:8px 14px;text-align:center;'
                    f'font-family:monospace;color:{nco_color};font-weight:700;">'
                    f'{nco_str}</td>'
                )
            else:
                net_co = cur_losses.get(key) if cur_losses else None
                if net_co is not None and cur_v:
                    nco_rate = net_co / cur_v
                    nco_color = "#922b21" if nco_rate > 0 else "#1e8449"
                    nco_str = f"{nco_rate:.2%}"
                else:
                    nco_color = "#555"
                    nco_str = "—"
                loss_td = (
                    f'<td style="padding:8px 14px;text-align:center;'
                    f'font-family:monospace;color:{nco_color};font-weight:600;">'
                    f'{nco_str}</td>'
                )

        if is_unfunded:
            label_html = (
                f'<span style="padding-left:20px;font-style:italic;color:#555;">'
                f'&#8627; {label}</span>'
            )
        else:
            label_html = label

        rows_html += (
            f'<tr style="background:{bg};{border}">'
            f'<td style="padding:8px 14px;font-weight:{weight};">{label_html}</td>'
            f'<td style="padding:8px 14px;text-align:right;font-family:monospace;'
            + ('font-style:italic;color:#555;">' if is_unfunded else '">')
            + f'{_fmt_dollars(ya_v)}</td>'
            f'<td style="padding:8px 14px;text-align:right;font-family:monospace;'
            + ('font-style:italic;color:#555;">' if is_unfunded else '">')
            + f'{_fmt_dollars(prior_v)}</td>'
            f'<td style="padding:8px 14px;text-align:right;font-family:monospace;font-weight:{weight};'
            + ('font-style:italic;color:#555;">' if is_unfunded else '">')
            + f'{_fmt_dollars(cur_v)}</td>'
            + rate_td
            + loss_td
            + (f'<td style="padding:8px 14px;"></td>'
               f'<td style="padding:8px 14px;"></td>'
               if is_unfunded else
               f'<td style="padding:8px 14px;text-align:center;color:{qoq_col};font-weight:600;">{qoq_txt}</td>'
               f'<td style="padding:8px 14px;text-align:center;color:{yoy_col};font-weight:600;">{yoy_txt}</td>')
            + f'</tr>\n'
        )

    return (
        f'<div class="card">'
        f'<h2>Loan Portfolio Breakdown '
        f'<span class="badge">{ya_label} · {prior_label} · {cur_label}</span></h2>'
        f'<div style="overflow-x:auto;">'
        f'<table>{header}<tbody>{rows_html}</tbody></table>'
        f'</div>'
        f'<p class="source-note" style="margin-top:10px;">'
        f'Source: NCUA 5300 FS220A / FS220B / FS220C / FS220H / FS220I / FS220L / FS220P &nbsp;|&nbsp; '
        f'QoQ = quarter-over-quarter vs {prior_label} &nbsp;|&nbsp; '
        f'YoY = year-over-year vs {ya_label}'
        + (
            f' &nbsp;|&nbsp; Loan Rate = rate reported to NCUA (FS220/FS220L/FS220H); '
            f'TOTAL row shows computed portfolio yield (annualised interest income ÷ total loans). '
            f'Unfunded Commitments row shows current draw-down utilisation (funded ÷ total commitment). '
            f'"—" = not reported (common for state-chartered CUs).'
            if show_rate else ''
        )
        + (
            f' &nbsp;|&nbsp; Annl. NCO% = annualised net charge-offs (charge-offs minus recoveries) '
            f'as % of loan balance; Q1×4, Q2×2, Q3×1.33, Q4×1. '
            f'Sources: FS220B (credit card), FS220I (vehicle), FS220H (PAL/student), FS220P (other categories). '
            f'"—" = not reported.'
            if show_loss else ''
        )
        + f'</p></div>'
    )


def extract_asset_classes(loans: dict) -> dict:
    """Aggregate loan keys into the 5 asset classes. Returns {class_name: total}."""
    result = {}
    for cls_name, keys in ASSET_CLASSES:
        result[cls_name] = sum(loans.get(k) or 0.0 for k in keys) or None
    # preserve total
    result["total"] = loans.get("total_loans")
    return result


def _make_asset_pie(ac: dict, label: str, dark_hdr: str) -> str:
    """Return Plotly pie chart HTML (no plotlyjs) for the given asset class dict."""
    import plotly.graph_objects as go
    import plotly.io as pio_local

    total = ac.get("total") or 1.0
    pie_labels, pie_values, pie_colors = [], [], []
    for (cls_name, _), color in zip(ASSET_CLASSES, ASSET_CLASS_COLORS):
        v = ac.get(cls_name) or 0.0
        pct = v / total * 100
        pie_labels.append(f"{cls_name} — {pct:.1f}%")
        pie_values.append(v)
        pie_colors.append(color)

    fig = go.Figure(go.Pie(
        labels=pie_labels,
        values=pie_values,
        marker=dict(colors=pie_colors, line=dict(color="white", width=2)),
        textinfo="label+percent",
        textposition="outside",
        hole=0.38,
        hovertemplate="<b>%{label}</b><br>$%{value:,.0f}<extra></extra>",
    ))
    fig.update_layout(
        title=dict(text=f"% Asset Class to Portfolio — {label}",
                   font=dict(size=14, color=dark_hdr), x=0.5, xanchor="center"),
        height=420,
        margin=dict(t=60, b=20, l=20, r=20),
        paper_bgcolor="white",
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=-0.18, xanchor="center", x=0.5),
        font=dict(family="Inter, 'Helvetica Neue', Arial, sans-serif", size=11),
    )
    return pio_local.to_html(
        fig, full_html=False, include_plotlyjs=False,
        config={"displayModeBar": False, "responsive": True},
    )


def build_asset_class_section(
    ya_label: str,  prior_label: str,  cur_label: str,
    ya_ac: dict,    prior_ac: dict,    cur_ac: dict,
) -> str:
    """Build the Asset Class summary table + two Plotly pie charts HTML card."""
    DARK_HDR = "#2c3e50"
    cur_total = cur_ac.get("total") or 1.0

    # ── Two pie charts (current quarter + year-ago quarter) ──────────────────
    pie_cur_html = _make_asset_pie(cur_ac, cur_label, DARK_HDR)
    pie_ya_html  = _make_asset_pie(ya_ac,  ya_label,  DARK_HDR)

    pies_html = (
        f'<div style="display:flex;gap:24px;flex-wrap:wrap;margin-bottom:24px;">'
        f'<div style="flex:1;min-width:300px;max-width:500px;">{pie_ya_html}</div>'
        f'<div style="flex:1;min-width:300px;max-width:500px;">{pie_cur_html}</div>'
        f'</div>'
    )

    # ── Summary table ────────────────────────────────────────────────────────
    header = (
        f'<thead><tr>'
        f'<th style="padding:9px 14px;background:{DARK_HDR};color:white;">Asset Class</th>'
        f'<th style="padding:9px 14px;background:{DARK_HDR};color:white;text-align:right;">{ya_label}</th>'
        f'<th style="padding:9px 14px;background:{DARK_HDR};color:white;text-align:right;">{prior_label}</th>'
        f'<th style="padding:9px 14px;background:{DARK_HDR};color:white;text-align:right;">{cur_label}</th>'
        f'<th style="padding:9px 14px;background:{DARK_HDR};color:white;text-align:center;">% of Portfolio</th>'
        f'<th style="padding:9px 14px;background:{DARK_HDR};color:white;text-align:center;">QoQ Change</th>'
        f'<th style="padding:9px 14px;background:{DARK_HDR};color:white;text-align:center;">YoY Change</th>'
        f'</tr></thead>'
    )

    rows_html = ""
    for (cls_name, _), color in zip(ASSET_CLASSES, ASSET_CLASS_COLORS):
        ya_v    = ya_ac.get(cls_name)
        prior_v = prior_ac.get(cls_name)
        cur_v   = cur_ac.get(cls_name)
        pct_str = f"{(cur_v / cur_total * 100):.2f}%" if cur_v else "—"
        qoq_txt, qoq_col = _fmt_change(cur_v, prior_v)
        yoy_txt, yoy_col = _fmt_change(cur_v, ya_v)
        dot = (f'<span style="display:inline-block;width:11px;height:11px;'
               f'border-radius:50%;background:{color};margin-right:7px;'
               f'vertical-align:middle;"></span>')
        rows_html += (
            f'<tr>'
            f'<td style="padding:8px 14px;">{dot}{cls_name}</td>'
            f'<td style="padding:8px 14px;text-align:right;font-family:monospace;">{_fmt_dollars(ya_v)}</td>'
            f'<td style="padding:8px 14px;text-align:right;font-family:monospace;">{_fmt_dollars(prior_v)}</td>'
            f'<td style="padding:8px 14px;text-align:right;font-family:monospace;font-weight:600;">{_fmt_dollars(cur_v)}</td>'
            f'<td style="padding:8px 14px;text-align:center;font-weight:600;">{pct_str}</td>'
            f'<td style="padding:8px 14px;text-align:center;color:{qoq_col};font-weight:600;">{qoq_txt}</td>'
            f'<td style="padding:8px 14px;text-align:center;color:{yoy_col};font-weight:600;">{yoy_txt}</td>'
            f'</tr>\n'
        )
    # Total row
    ya_t    = ya_ac.get("total")
    prior_t = prior_ac.get("total")
    cur_t   = cur_ac.get("total")
    qoq_txt, qoq_col = _fmt_change(cur_t, prior_t)
    yoy_txt, yoy_col = _fmt_change(cur_t, ya_t)
    rows_html += (
        f'<tr style="background:#f0f4f8;border-top:2px solid #dee2e6;">'
        f'<td style="padding:8px 14px;font-weight:700;">TOTAL</td>'
        f'<td style="padding:8px 14px;text-align:right;font-family:monospace;">{_fmt_dollars(ya_t)}</td>'
        f'<td style="padding:8px 14px;text-align:right;font-family:monospace;">{_fmt_dollars(prior_t)}</td>'
        f'<td style="padding:8px 14px;text-align:right;font-family:monospace;font-weight:700;">{_fmt_dollars(cur_t)}</td>'
        f'<td style="padding:8px 14px;text-align:center;font-weight:700;">100.00%</td>'
        f'<td style="padding:8px 14px;text-align:center;color:{qoq_col};font-weight:600;">{qoq_txt}</td>'
        f'<td style="padding:8px 14px;text-align:center;color:{yoy_col};font-weight:600;">{yoy_txt}</td>'
        f'</tr>\n'
    )

    return (
        f'<div class="card">'
        f'<h2>Loan Portfolio by Asset Class '
        f'<span class="badge">{ya_label} · {prior_label} · {cur_label}</span></h2>'
        f'{pies_html}'
        f'<div style="width:100%;overflow-x:auto;">'
        f'<table style="width:100%;border-collapse:collapse;">'
        f'{header}<tbody>{rows_html}</tbody></table>'
        f'</div>'
        f'<p class="source-note" style="margin-top:10px;">'
        f'Asset classes aggregated from NCUA 5300 loan schedule &nbsp;|&nbsp; '
        f'QoQ = quarter-over-quarter vs {prior_label} &nbsp;|&nbsp; '
        f'YoY = year-over-year vs {ya_label}'
        f'</p></div>'
    )


# ─────────────────────────────────────────────────────────────────────────────
# Ratio calculation
# ─────────────────────────────────────────────────────────────────────────────

def _get(record: dict, key: str) -> Optional[float]:
    """Retrieve a financial value by trying all alias column names for *key*."""
    for alias in ACCT.get(key, []):
        val = record.get(alias)
        if val is not None and str(val).strip() not in ("", "N/A", "NA"):
            try:
                return float(str(val).replace(",", "").replace("$", "").strip())
            except ValueError:
                pass
    return None


def _div(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b is None or b == 0.0:
        return None
    return a / b


def calc_ratios(record: dict, prev: Optional[dict] = None) -> dict:
    """Compute all 8 financial ratios from one quarter's raw record."""
    yr, mo = record["_year"], record["_month"]
    af = ann_factor(mo)

    ta      = _get(record, "total_assets")
    tl      = _get(record, "total_loans")
    ts      = _get(record, "total_shares")
    nw      = _get(record, "net_worth")
    ni      = _get(record, "net_income_ytd")
    dq      = _get(record, "delinquent")
    opex    = _get(record, "opex_ytd")
    gross   = _get(record, "gross_chargeoffs_ytd")
    rec     = _get(record, "recoveries_ytd")
    int_inc   = _get(record, "interest_income_ytd")
    inv_inc   = _get(record, "invest_income_ytd")
    funding   = _get(record, "funding_costs_ytd")
    fee_inc   = _get(record, "fee_income_ytd")
    gain_inc  = _get(record, "gain_on_assets_ytd")
    nonop_inc = _get(record, "other_nonop_income_ytd")
    # Sum both other-income components; treat missing as 0 only if at least one is present
    oth_inc   = (
        (gain_inc or 0.0) + (nonop_inc or 0.0)
        if (gain_inc is not None or nonop_inc is not None) else None
    )
    inv_cash  = _get(record, "invest_cash_deposits")
    inv_sec   = _get(record, "invest_securities")
    inv_other = _get(record, "invest_other")
    inv_730a  = _get(record, "invest_cash_730a")
    inv_730b  = _get(record, "invest_cash_730b")
    inv_short = _get(record, "invest_short_term")

    # Net charge-offs = gross charge-offs minus recoveries
    co = (gross - (rec or 0.0)) if gross is not None else None

    # Net Interest Income YTD (all components are YTD; annualise together)
    total_int_inc = (int_inc or 0.0) + (inv_inc or 0.0)
    nii_ytd = (total_int_inc - (funding or 0.0)) if (int_inc is not None or inv_inc is not None) else None

    # Non-Interest Income YTD — use NCUA's official total (ACCT_117) when available;
    # it captures all components including insurance income, mortgage banking, etc.
    # Fall back to summing known sub-components for CUs or quarters where ACCT_117 is absent.
    _total_nonint = _get(record, "total_nonint_income_ytd")
    non_int_inc_ytd = (
        _total_nonint
        if _total_nonint is not None
        else (fee_inc or 0.0) + (oth_inc or 0.0) if fee_inc is not None else None
    )

    borrowings = _get(record, "total_borrowings")

    r: dict = {
        "quarter":        ql(yr, mo),
        "_year":          yr,
        "_month":         mo,
        "_total_assets":  ta,
        "_total_shares":  ts,
        "_total_borrowings": borrowings,
        "net_worth_ratio":  _div(nw, ta),
        "roa":              _div(None if ni is None else ni * af, ta),
        "loan_to_share":    _div(tl, ts),
        "delinquency_ratio": _div(dq, tl),
        "charge_off_ratio": _div(None if co is None else co * af, tl),
        "opex_ratio":       _div(None if opex is None else opex * af, ta),
    }

    # NIM: annualised net interest income / total assets
    r["net_interest_margin"] = _div(
        nii_ytd * af if nii_ytd is not None else None, ta
    )

    # NIM excluding investment income — isolates loan yield vs. funding cost.
    # When investment yield is unusually high it can flatter overall NIM; this
    # field exposes the loan-only contribution so the recommendation module can
    # flag cases where the lending margin is weaker than NIM implies.
    r["nim_ex_investments"] = _div(
        ((int_inc or 0.0) - (funding or 0.0)) * af if int_inc is not None else None,
        ta,
    )

    # Investment Yield: annualised investment income / total investment portfolio
    # Portfolio = cash & deposits + investment securities + other investments
    total_invest = (inv_cash or 0.0) + (inv_sec or 0.0) + (inv_other or 0.0)
    r["investment_yield"] = _div(
        inv_inc * af if inv_inc is not None else None,
        total_invest if total_invest > 0 else None,
    )

    # Cost of Funds: annualised dividends & interest expense / total shares & deposits
    r["cost_of_funds"] = _div(
        funding * af if funding is not None else None, ts
    )

    # Liquidity Ratio: (cash + investments maturing < 1 yr) / total assets
    # Uses 730A+730B (FS220A cash) + NV0153 (FS220Q short-term investments) per NCUA FPR methodology
    _have_st = inv_730a is not None or inv_730b is not None or inv_short is not None
    liquid_assets = (inv_730a or 0.0) + (inv_730b or 0.0) + (inv_short or 0.0)
    r["liquidity_ratio"] = _div(liquid_assets if _have_st else None, ta)

    # Efficiency ratio: opex / (net interest income + non-interest income)
    # Uses YTD figures — annualisation cancels out in numerator and denominator
    if opex is not None and nii_ytd is not None:
        total_revenue_ytd = nii_ytd + (non_int_inc_ytd or 0.0)
        r["efficiency_ratio"] = _div(opex, total_revenue_ytd)
    else:
        r["efficiency_ratio"] = None

    r["interest_income_ann"] = (total_int_inc * af) if (int_inc is not None or inv_inc is not None) else None
    r["nonint_income_ann"]   = (non_int_inc_ytd * af) if non_int_inc_ytd is not None else None
    r["net_income"] = ni * af if ni is not None else None

    if prev is not None:
        p_ta = _get(prev, "total_assets")
        p_ts = _get(prev, "total_shares")
        r["asset_growth"] = (
            _div(ta - p_ta, p_ta) * 4.0
            if ta is not None and p_ta is not None and p_ta != 0 else None
        )
        r["share_growth"] = (
            _div(ts - p_ts, p_ts) * 4.0
            if ts is not None and p_ts is not None and p_ts != 0 else None
        )
    else:
        r["asset_growth"] = None
        r["share_growth"] = None

    return r


# ─────────────────────────────────────────────────────────────────────────────
# Colour coding
# ─────────────────────────────────────────────────────────────────────────────

def traffic_light(ratio_key: str, value: Optional[float]) -> str:
    if value is None:
        return NEUTRAL
    info = RATIOS.get(ratio_key, {})
    bm   = info.get("benchmark")
    d    = info.get("direction", "higher")
    if bm is None:
        return NEUTRAL
    if d == "higher":
        return GREEN if value >= bm else (YELLOW if value >= bm * 0.85 else RED)
    if d == "lower":
        return GREEN if value <= bm else (YELLOW if value <= bm * 2.0 else RED)
    return NEUTRAL   # neutral (loan-to-share)


def fv(ratio_key: str, v: Optional[float]) -> str:
    """Format a ratio value for display."""
    if v is None:
        return "N/A"
    info = RATIOS[ratio_key]
    return info.get("prefix", "") + format(v, info["fmt"])


# ─────────────────────────────────────────────────────────────────────────────
# ChatGPT analysis
# ─────────────────────────────────────────────────────────────────────────────

def gpt_analysis(cu_name: str, ratios: list[dict]) -> str:
    """Send ratio data to ChatGPT and return a markdown analysis string."""
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        return (
            "**AI Analysis Not Available**\n\n"
            "Set the `OPENAI_API_KEY` environment variable and re-run "
            "to enable ChatGPT commentary."
        )
    try:
        from openai import OpenAI
    except ImportError:
        return "**`openai` package not installed** — run `pip install openai`."

    # Build structured data table for the prompt
    lines = [
        f"You are a senior credit union financial analyst. "
        f"Below is three quarters of NCUA 5300 call report data for **{cu_name}**.\n"
    ]
    for r in ratios:
        lines.append(f"### {r['quarter']}")
        for k, info in RATIOS.items():
            lines.append(f"- {info['label']}: {fv(k, r.get(k))}")
        lines.append("")

    lines += [
        "Please provide a concise analysis with these sections:\n",
        "1. **Executive Summary** (2–3 sentences on overall financial health)",
        "2. **Key Strengths** (bullet points)",
        "3. **Areas of Concern** (bullet points)",
        "4. **Trend Observations** (what is improving, deteriorating, or stable)",
        "5. **Recommendations** (actionable items for management)\n",
        "Reference NCUA peer benchmarks where relevant. Be specific and data-driven.",
    ]

    print("  Calling ChatGPT (gpt-4o) for analysis…")
    try:
        client = OpenAI(api_key=api_key)
        resp = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a senior credit union financial analyst with deep expertise in "
                        "NCUA regulatory metrics, CAMEL ratings, liquidity management, "
                        "and industry benchmarking for federally insured credit unions."
                    ),
                },
                {"role": "user", "content": "\n".join(lines)},
            ],
            temperature=0.2,
            max_tokens=1600,
        )
        return resp.choices[0].message.content
    except Exception as e:
        return f"**ChatGPT call failed:** {e}"


# ─────────────────────────────────────────────────────────────────────────────
# Dashboard builder
# ─────────────────────────────────────────────────────────────────────────────

def _safe_export_base(cu_name: str) -> str:
    return re.sub(r"[^\w\s-]", "", cu_name).strip().replace(" ", "_") or "credit_union_dashboard"


def _md_escape(value: object) -> str:
    return str(value if value is not None else "").replace("|", "\\|").replace("\n", " ")


def _plain_text_from_html(html_text: str) -> str:
    """Convert generated dashboard HTML snippets into readable text for exports."""
    if not html_text:
        return ""
    text = re.sub(r"(?i)<br\s*/?>", "\n", html_text)
    text = re.sub(r"(?i)</(p|div|li|h[1-6]|tr)>", "\n", text)
    text = re.sub(r"<[^>]+>", "", text)
    text = html_lib.unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _fmt_pct(v: Optional[float], digits: int = 2) -> str:
    return "N/A" if v is None else f"{v:.{digits}%}"


def _fmt_raw(v: Optional[float]) -> str:
    if v is None or v == "":
        return ""
    try:
        return f"{float(v):.10g}"
    except (TypeError, ValueError):
        return str(v)


def _json_safe(value):
    """Recursively coerce export values into JSON-safe primitives."""
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _markdown_table(headers: list[str], rows: list[list[object]]) -> str:
    lines = [
        "| " + " | ".join(_md_escape(h) for h in headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(_md_escape(v) for v in row) + " |")
    return "\n".join(lines)


def _portfolio_md_rows(
    categories: list[tuple[str, str]],
    old_vals: dict,
    prior_vals: dict,
    cur_vals: dict,
    old_label: str,
    prior_label: str,
    cur_label: str,
    value_formatter,
    current_extras: Optional[dict[str, str]] = None,
) -> list[list[object]]:
    rows = []
    for key, label in categories:
        cur_v = cur_vals.get(key)
        qoq, _ = _fmt_change(cur_v, prior_vals.get(key))
        yoy, _ = _fmt_change(cur_v, old_vals.get(key))
        row = [
            label,
            value_formatter(old_vals.get(key)),
            value_formatter(prior_vals.get(key)),
            value_formatter(cur_v),
            qoq,
            yoy,
        ]
        if current_extras:
            row.append(current_extras.get(key, ""))
        rows.append(row)
    return rows


def build_export_artifacts(
    cu_name: str,
    ratios: list[dict],
    analysis: str,
    cu_meta: Optional[dict],
    export_data: dict,
    upstart_rec: Optional[dict],
    sales_questions_html: str = "",
) -> dict:
    """
    Build deterministic, structured exports embedded in the generated dashboard.

    Markdown is optimized for another LLM or agent to ingest the CU's situation.
    CSV is long-form so coworkers can filter/pivot metrics without scraping HTML.
    HTML/PDF are handled in-browser so the visual dashboard stays faithful.
    """
    file_base = _safe_export_base(cu_name)
    generated = date.today().isoformat()
    quarters = [r.get("quarter", "") for r in ratios]
    latest = ratios[-1] if ratios else {}

    md: list[str] = [
        f"# {cu_name} NCUA 5300 Dashboard Export",
        "",
        f"- Generated: {generated}",
        f"- Reporting quarters: {quarters[0] if quarters else 'N/A'} to {quarters[-1] if quarters else 'N/A'}",
        "- Intended use: structured context for analysis by another model, agent, or analyst.",
        "- Source: NCUA 5300 Call Report bulk data; HMDA section uses FFIEC Data Browser when available.",
        "",
        "## Credit Union Profile",
    ]

    if cu_meta:
        profile_rows = [
            ["CU Name", cu_name],
            ["Charter Number", cu_meta.get("cu_number", "")],
            ["Location", f"{cu_meta.get('city', '')} {cu_meta.get('state', '')}".strip()],
            ["Charter Type", cu_meta.get("cu_type", "")],
            ["Field of Membership", cu_meta.get("fom", "")],
            ["Members", f"{cu_meta.get('members', ''):,}" if isinstance(cu_meta.get("members"), int) else cu_meta.get("members", "")],
            ["Branches", cu_meta.get("num_branches", "")],
            ["CEO", cu_meta.get("ceo_name", "")],
            ["Year Opened", cu_meta.get("year_opened", "")],
            ["Low-Income Designated", "Yes" if cu_meta.get("low_income") else "No"],
        ]
        md.append(_markdown_table(["Field", "Value"], profile_rows))
    else:
        md.append("No CU metadata was available.")

    if upstart_rec:
        md.extend([
            "",
            "## Upstart Partnership Recommendation",
            "",
            _markdown_table(
                ["Field", "Value"],
                [
                    ["Overall", upstart_rec.get("overall", "")],
                    ["Confidence", upstart_rec.get("confidence", "")],
                    ["Score", upstart_rec.get("score", "")],
                    ["Data Points", upstart_rec.get("data_points", "")],
                    ["Recommended Products", ", ".join(upstart_rec.get("products", [])) or "None"],
                ],
            ),
        ])
        if upstart_rec.get("products"):
            md.append("\n### Product Rationale")
            prod_rows = [
                [
                    i + 1,
                    product,
                    upstart_rec.get("product_scores", {}).get(product, ""),
                    upstart_rec.get("product_reasoning", {}).get(product, ""),
                ]
                for i, product in enumerate(upstart_rec.get("products", []))
            ]
            md.append(_markdown_table(["Rank", "Product", "Score", "Rationale"], prod_rows))
        if upstart_rec.get("signals"):
            md.append("\n### Supporting Factors")
            for i, signal in enumerate(upstart_rec.get("signals", []), 1):
                weight = ""
                weights = upstart_rec.get("signal_weights", [])
                if i - 1 < len(weights):
                    weight = f" (impact {weights[i - 1]})"
                md.append(f"{i}. {signal}{weight}")
        if upstart_rec.get("concerns"):
            md.append("\n### Considerations and Cautions")
            for concern in upstart_rec.get("concerns", []):
                md.append(f"- {concern}")
        if upstart_rec.get("rationale"):
            md.extend(["", "### Full Rationale", upstart_rec["rationale"]])

    md.extend(["", "## Key Ratio Summary"])
    ratio_headers = ["Metric", "Description", "Benchmark"] + quarters
    ratio_rows_md = []
    for rk, info in RATIOS.items():
        ratio_rows_md.append(
            [info["label"], info["desc"], info["bm_label"]]
            + [fv(rk, r.get(rk)) for r in ratios]
        )
    md.append(_markdown_table(ratio_headers, ratio_rows_md))

    if latest:
        md.extend(["", "## Latest Quarter Raw Snapshot"])
        latest_rows = [
            ["Total Assets", _fmt_dollars(latest.get("_total_assets"))],
            ["Total Shares and Deposits", _fmt_dollars(latest.get("_total_shares"))],
            ["Total Borrowings", _fmt_dollars(latest.get("_total_borrowings"))],
            ["Latest Quarter", latest.get("quarter", "")],
        ]
        md.append(_markdown_table(["Metric", "Value"], latest_rows))

    labels = export_data.get("labels", {})
    old_label = labels.get("old", "Oldest")
    prior_label = labels.get("prior", "Prior")
    cur_label = labels.get("current", "Current")

    shares = export_data.get("shares")
    if shares:
        md.extend(["", "## Shares and Deposits"])
        rows = _portfolio_md_rows(
            SHARE_CATEGORIES,
            shares.get("old", {}),
            shares.get("prior", {}),
            shares.get("current", {}),
            old_label,
            prior_label,
            cur_label,
            _fmt_dollars,
        )
        md.append(_markdown_table(["Category", old_label, prior_label, cur_label, "QoQ", "YoY"], rows))

    loans = export_data.get("loans")
    if loans:
        current_loans = loans.get("current", {})
        cur_rates = export_data.get("loan_rates", {})
        cur_losses = export_data.get("loan_losses", {})
        extras: dict[str, str] = {}
        for key, _ in LOAN_CATEGORIES:
            notes = []
            rate_key = LOAN_RATE_KEY.get(key)
            if rate_key and cur_rates.get(rate_key) is not None:
                notes.append(f"rate {_fmt_pct(cur_rates.get(rate_key), 2)}")
            if key == "total_loans" and export_data.get("portfolio_yield") is not None:
                notes.append(f"portfolio yield {_fmt_pct(export_data.get('portfolio_yield'), 2)}")
            loss = cur_losses.get(key)
            balance = current_loans.get(key)
            if loss is not None and balance:
                notes.append(f"NCO {_fmt_pct(loss / balance, 2)}")
            if key == "total_loans" and export_data.get("portfolio_nco") is not None:
                notes.append(f"portfolio NCO {_fmt_pct(export_data.get('portfolio_nco'), 2)}")
            extras[key] = "; ".join(notes)
        md.extend(["", "## Loan Portfolio"])
        rows = _portfolio_md_rows(
            LOAN_CATEGORIES,
            loans.get("old", {}),
            loans.get("prior", {}),
            current_loans,
            old_label,
            prior_label,
            cur_label,
            _fmt_dollars,
            extras,
        )
        md.append(_markdown_table(["Category", old_label, prior_label, cur_label, "QoQ", "YoY", "Current Notes"], rows))

    investments = export_data.get("investments")
    if investments:
        md.extend(["", "## Investment Portfolio"])
        if export_data.get("investment_yield") is not None:
            md.append(f"- Current portfolio yield: {_fmt_pct(export_data.get('investment_yield'), 2)}")
        type_rows = _portfolio_md_rows(
            INVEST_TYPE_CATEGORIES,
            investments.get("old", {}),
            investments.get("prior", {}),
            investments.get("current", {}),
            old_label,
            prior_label,
            cur_label,
            _fmt_dollars,
        )
        md.append(_markdown_table(["Investment Type", old_label, prior_label, cur_label, "QoQ", "YoY"], type_rows))
        maturity_rows = []
        cur_total = investments.get("current", {}).get("total_invest") or 0.0
        for key, label in INVEST_MATURITY_BUCKETS:
            cur_v = investments.get("current", {}).get(key)
            maturity_rows.append([
                label,
                _fmt_dollars(investments.get("old", {}).get(key)),
                _fmt_dollars(investments.get("prior", {}).get(key)),
                _fmt_dollars(cur_v),
                f"{cur_v / cur_total:.1%}" if cur_v is not None and cur_total else "N/A",
            ])
        md.append("\n### Investment Maturity Schedule")
        md.append(_markdown_table(["Bucket", old_label, prior_label, cur_label, "% of Current Portfolio"], maturity_rows))

    asset_classes = export_data.get("asset_classes")
    if asset_classes:
        md.extend(["", "## Loan Portfolio by Asset Class"])
        cur_total = asset_classes.get("current", {}).get("total") or 0.0
        ac_rows = []
        for cls_name, _ in ASSET_CLASSES:
            cur_v = asset_classes.get("current", {}).get(cls_name)
            qoq, _ = _fmt_change(cur_v, asset_classes.get("prior", {}).get(cls_name))
            yoy, _ = _fmt_change(cur_v, asset_classes.get("old", {}).get(cls_name))
            ac_rows.append([
                cls_name,
                _fmt_dollars(asset_classes.get("old", {}).get(cls_name)),
                _fmt_dollars(asset_classes.get("prior", {}).get(cls_name)),
                _fmt_dollars(cur_v),
                f"{cur_v / cur_total:.2%}" if cur_v and cur_total else "N/A",
                qoq,
                yoy,
            ])
        md.append(_markdown_table(["Asset Class", old_label, prior_label, cur_label, "% of Portfolio", "QoQ", "YoY"], ac_rows))

    hmda = export_data.get("hmda")
    if hmda and hmda.get("found"):
        md.extend(["", "## HMDA Mortgage Originations"])
        hmda_rows = [
            ["Year", hmda.get("year", "")],
            ["LEI", hmda.get("lei", "")],
            ["Originations", f"{hmda.get('total_count', 0):,}"],
            ["Volume", _fmt_dollars(hmda.get("total_sum"))],
        ]
        md.append(_markdown_table(["Metric", "Value"], hmda_rows))

    md.extend(["", "## AI Analysis", analysis.strip() or "No AI analysis was generated."])

    sales_text = _plain_text_from_html(sales_questions_html)
    if sales_text:
        md.extend(["", "## Sales Conversation Guide", sales_text])

    md.extend([
        "",
        "## Notes",
        "- Growth figures are annualized quarter-over-quarter changes.",
        "- Q1 YTD figures are annualized x4, Q2 x2, Q3 x1.33, and Q4 x1.",
        "- This export is informational only and is not investment or regulatory advice.",
    ])

    csv_rows: list[dict[str, object]] = []

    def add_csv(section: str, subsection: str, item: str, description: str = "",
                benchmark: str = "", quarter: str = "", value: object = "",
                formatted_value: str = "", rate: object = "", nco_rate: object = "",
                qoq_change: str = "", yoy_change: str = "", notes: str = "") -> None:
        csv_rows.append({
            "section": section,
            "subsection": subsection,
            "item": item,
            "description": description,
            "benchmark": benchmark,
            "quarter": quarter,
            "value": value,
            "formatted_value": formatted_value,
            "rate": rate,
            "nco_rate": nco_rate,
            "qoq_change": qoq_change,
            "yoy_change": yoy_change,
            "notes": notes,
        })

    if cu_meta:
        for field, value in profile_rows:
            add_csv("profile", "", field, value=value, formatted_value=str(value))

    for rk, info in RATIOS.items():
        for r in ratios:
            add_csv(
                "ratios", "",
                info["label"],
                description=info["desc"],
                benchmark=info["bm_label"],
                quarter=r.get("quarter", ""),
                value=_fmt_raw(r.get(rk)),
                formatted_value=fv(rk, r.get(rk)),
                notes=f"direction={info.get('direction', '')}",
            )

    def add_balance_rows(section: str, categories: list[tuple[str, str]], values_by_period: dict,
                         formatter=_fmt_dollars) -> None:
        period_map = [
            ("old", old_label),
            ("prior", prior_label),
            ("current", cur_label),
        ]
        for key, label in categories:
            cur_v = values_by_period.get("current", {}).get(key)
            qoq, _ = _fmt_change(cur_v, values_by_period.get("prior", {}).get(key))
            yoy, _ = _fmt_change(cur_v, values_by_period.get("old", {}).get(key))
            for period_key, label_q in period_map:
                add_csv(
                    section, key, label,
                    quarter=label_q,
                    value=_fmt_raw(values_by_period.get(period_key, {}).get(key)),
                    formatted_value=formatter(values_by_period.get(period_key, {}).get(key)),
                    qoq_change=qoq if period_key == "current" else "",
                    yoy_change=yoy if period_key == "current" else "",
                )

    if shares:
        add_balance_rows("shares_deposits", SHARE_CATEGORIES, shares)
    if investments:
        add_balance_rows("investment_types", INVEST_TYPE_CATEGORIES, investments)
        add_balance_rows("investment_maturity", INVEST_MATURITY_BUCKETS, investments)
    if asset_classes:
        add_balance_rows("asset_classes", [(name, name) for name, _ in ASSET_CLASSES], asset_classes)
    if loans:
        for key, label in LOAN_CATEGORIES:
            cur_v = loans.get("current", {}).get(key)
            qoq, _ = _fmt_change(cur_v, loans.get("prior", {}).get(key))
            yoy, _ = _fmt_change(cur_v, loans.get("old", {}).get(key))
            rate_key = LOAN_RATE_KEY.get(key)
            rate = export_data.get("loan_rates", {}).get(rate_key) if rate_key else ""
            loss = export_data.get("loan_losses", {}).get(key)
            nco_rate = (loss / cur_v) if loss is not None and cur_v else ""
            for period_key, label_q in [("old", old_label), ("prior", prior_label), ("current", cur_label)]:
                add_csv(
                    "loan_portfolio", key, label,
                    quarter=label_q,
                    value=_fmt_raw(loans.get(period_key, {}).get(key)),
                    formatted_value=_fmt_dollars(loans.get(period_key, {}).get(key)),
                    rate=_fmt_raw(rate) if period_key == "current" else "",
                    nco_rate=_fmt_raw(nco_rate) if period_key == "current" else "",
                    qoq_change=qoq if period_key == "current" else "",
                    yoy_change=yoy if period_key == "current" else "",
                )

    if upstart_rec:
        add_csv("upstart_recommendation", "", "overall", value=upstart_rec.get("overall", ""))
        add_csv("upstart_recommendation", "", "confidence", value=upstart_rec.get("confidence", ""))
        for i, product in enumerate(upstart_rec.get("products", []), 1):
            add_csv(
                "upstart_products", f"rank_{i}", product,
                value=upstart_rec.get("product_scores", {}).get(product, ""),
                notes=upstart_rec.get("product_reasoning", {}).get(product, ""),
            )
        for i, signal in enumerate(upstart_rec.get("signals", []), 1):
            weight = upstart_rec.get("signal_weights", [""] * len(upstart_rec.get("signals", [])))
            add_csv("upstart_signals", f"rank_{i}", signal, value=weight[i - 1] if i - 1 < len(weight) else "")
        for i, concern in enumerate(upstart_rec.get("concerns", []), 1):
            add_csv("upstart_concerns", f"item_{i}", concern)

    if hmda and hmda.get("found"):
        add_csv("hmda", "summary", "originations", quarter=str(hmda.get("year", "")),
                value=hmda.get("total_count", ""), formatted_value=f"{hmda.get('total_count', 0):,}",
                notes=f"LEI={hmda.get('lei', '')}")
        add_csv("hmda", "summary", "volume", quarter=str(hmda.get("year", "")),
                value=_fmt_raw(hmda.get("total_sum")), formatted_value=_fmt_dollars(hmda.get("total_sum")))
        for agg in hmda.get("by_loan_type", []):
            code = str(agg.get("loan_types", ""))
            add_csv("hmda", "loan_type", HMDA_LOAN_TYPES.get(code, code),
                    quarter=str(hmda.get("year", "")), value=agg.get("count", ""),
                    formatted_value=str(agg.get("count", "")), notes=f"volume={agg.get('sum', '')}")
        for agg in hmda.get("by_loan_purpose", []):
            code = str(agg.get("loan_purposes", ""))
            add_csv("hmda", "loan_purpose", HMDA_LOAN_PURPOSES.get(code, code),
                    quarter=str(hmda.get("year", "")), value=agg.get("count", ""),
                    formatted_value=str(agg.get("count", "")), notes=f"volume={agg.get('sum', '')}")

    csv_buf = io.StringIO()
    csv_columns = [
        "section", "subsection", "item", "description", "benchmark", "quarter",
        "value", "formatted_value", "rate", "nco_rate", "qoq_change", "yoy_change", "notes",
    ]
    writer = csv.DictWriter(csv_buf, fieldnames=csv_columns)
    writer.writeheader()
    writer.writerows(csv_rows)

    structured_payload = {
        "schema_version": "1.0",
        "generated": generated,
        "credit_union": _json_safe(cu_meta or {"cu_name": cu_name}),
        "reporting_quarters": quarters,
        "ratios": [
            {
                "key": rk,
                "label": info["label"],
                "description": info["desc"],
                "benchmark": info.get("benchmark"),
                "benchmark_label": info.get("bm_label"),
                "direction": info.get("direction"),
                "values": [
                    {
                        "quarter": r.get("quarter"),
                        "raw_value": r.get(rk),
                        "formatted_value": fv(rk, r.get(rk)),
                    }
                    for r in ratios
                ],
            }
            for rk, info in RATIOS.items()
        ],
        "latest_quarter": _json_safe(latest),
        "portfolio_sections": _json_safe(export_data),
        "upstart_recommendation": _json_safe(upstart_rec or {}),
        "ai_analysis_markdown": analysis.strip(),
        "sales_conversation_guide_text": sales_text,
        "notes": [
            "Growth figures are annualized quarter-over-quarter changes.",
            "Q1 YTD figures are annualized x4, Q2 x2, Q3 x1.33, and Q4 x1.",
            "This export is informational only and is not investment or regulatory advice.",
        ],
    }

    return {
        "fileBase": file_base,
        "generated": generated,
        "markdown": "\n".join(md).strip() + "\n",
        "csv": csv_buf.getvalue(),
        "json": json.dumps(structured_payload, indent=2, ensure_ascii=False),
    }


def _md(text: str) -> str:
    """Minimal Markdown → HTML converter for the analysis panel."""
    # Headers
    text = re.sub(r"^###\s+(.*)", r"<h4>\1</h4>", text, flags=re.MULTILINE)
    text = re.sub(r"^##\s+(.*)",  r"<h3>\1</h3>", text, flags=re.MULTILINE)
    text = re.sub(r"^#\s+(.*)",   r"<h3>\1</h3>", text, flags=re.MULTILINE)
    # Bold
    text = re.sub(r"\*\*(.*?)\*\*", r"<strong>\1</strong>", text)
    # Bullet lists
    text = re.sub(r"^\s*[-*]\s+(.+)", r"<li>\1</li>", text, flags=re.MULTILINE)
    text = re.sub(r"(<li>.*?</li>\n)+", r"<ul>\g<0></ul>", text, flags=re.DOTALL)
    # Numbered lists
    text = re.sub(r"^\d+\.\s+(.+)", r"<li>\1</li>", text, flags=re.MULTILINE)
    # Paragraphs
    text = re.sub(r"\n{2,}", r"</p><p>", text)
    return f"<p>{text}</p>"


def build_dashboard(
    cu_name: str,
    ratios: list[dict],     # oldest → newest
    analysis: str,
    out_path: str,
    cu_meta: Optional[dict] = None,
    shares_html: str = "",
    loans_html: str = "",
    investments_html: str = "",
    asset_class_html: str = "",
    hmda_html: str = "",
    upstart_top_html: str = "",    # DREW3: recommendation banner (top)
    upstart_bottom_html: str = "", # DREW3: rationale card (bottom)
    sales_questions_html: str = "", # DREW3.2: conversation starter questions
    export_artifacts: Optional[dict] = None,
) -> None:
    ratio_keys  = list(RATIOS.keys())
    n_ratios    = len(ratio_keys)
    cols        = 4
    n_rows      = -(-n_ratios // cols)    # ceiling division
    quarters    = [r["quarter"] for r in ratios]

    # ── Subplot grid ────────────────────────────────────────────────────────
    sub_titles = [RATIOS[k]["label"] for k in ratio_keys]
    while len(sub_titles) < n_rows * cols:
        sub_titles.append("")

    fig = make_subplots(
        rows=n_rows, cols=cols,
        subplot_titles=sub_titles,
        vertical_spacing=0.16,
        horizontal_spacing=0.07,
    )

    # ── One bar+trend chart per ratio ────────────────────────────────────────
    for idx, rk in enumerate(ratio_keys):
        row = idx // cols + 1
        col = idx % cols + 1
        info   = RATIOS[rk]
        fmt    = info["fmt"]
        values = [r.get(rk) for r in ratios]
        colors = [traffic_light(rk, v) for v in values]
        texts  = [format(v, fmt) if v is not None else "N/A" for v in values]

        # Bars
        fig.add_trace(go.Bar(
            x=quarters, y=values,
            text=texts, textposition="outside",
            textfont=dict(size=10, color=DARK),
            marker=dict(color=colors, opacity=0.85,
                        line=dict(color="white", width=1.5)),
            name=info["label"], showlegend=False,
            hovertemplate=(
                f"<b>{info['label']}</b><br>"
                "%{x}: <b>%{text}</b><extra></extra>"
            ),
        ), row=row, col=col)

        # Trend line
        valid = [(i, v) for i, v in enumerate(values) if v is not None]
        if len(valid) >= 2:
            fig.add_trace(go.Scatter(
                x=[quarters[i] for i, _ in valid],
                y=[v for _, v in valid],
                mode="lines+markers",
                line=dict(color=DARK, width=2, dash="dot"),
                marker=dict(size=7, color=DARK, symbol="circle"),
                showlegend=False, hoverinfo="skip",
            ), row=row, col=col)

        # Benchmark dashed line
        bm = info.get("benchmark")
        if bm is not None:
            fig.add_hline(
                y=bm,
                line_dash="dash", line_color="rgba(80,80,80,0.40)",
                annotation_text=info.get("bm_label", format(bm, fmt)),
                annotation_font_size=9,
                annotation_position="top right",
                row=row, col=col,
            )

        fig.update_yaxes(
            tickformat=fmt, tickfont_size=9,
            showgrid=True, gridcolor="#e8ecf0", gridwidth=1,
            zeroline=True, zerolinecolor="#ccc",
            row=row, col=col,
        )
        fig.update_xaxes(tickfont_size=9, row=row, col=col)

    fig.update_layout(
        height=n_rows * 310 + 60,
        margin=dict(t=80, b=40, l=55, r=60),
        paper_bgcolor="white",
        plot_bgcolor="#f8f9fa",
        font=dict(family="Inter, 'Helvetica Neue', Arial, sans-serif",
                  size=11, color=DARK),
        bargap=0.25,
    )

    chart_html = pio.to_html(
        fig, full_html=False, include_plotlyjs=False,
        config={"displayModeBar": False, "responsive": True},
    )

    # ── Summary table ────────────────────────────────────────────────────────
    th_qs = "".join(
        f'<th style="background:{DARK};color:white;'
        f'padding:9px 16px;text-align:center;">{q}</th>'
        for q in quarters
    )
    tbody = ""
    for rk, info in RATIOS.items():
        tds = ""
        for r in ratios:
            v   = r.get(rk)
            bg  = traffic_light(rk, v)
            val = fv(rk, v)
            tds += (
                f'<td style="text-align:center;padding:8px 16px;'
                f'background:{bg}1a;font-weight:600;">{val}</td>'
            )
        tbody += (
            f"<tr>"
            f'<td style="padding:8px 14px;font-weight:500;">{info["label"]}</td>'
            f'<td style="padding:8px 14px;font-size:.82rem;color:#6c7a89;">'
            f'{info["desc"]}</td>'
            f'<td style="padding:8px 14px;font-size:.82rem;color:#6c7a89;'
            f'text-align:center;">{info["bm_label"]}</td>'
            f"{tds}"
            f"</tr>\n"
        )

    # ── Legend ───────────────────────────────────────────────────────────────
    def dot(c: str, label: str) -> str:
        return (
            f'<span style="display:inline-flex;align-items:center;gap:5px;">'
            f'<span style="width:13px;height:13px;border-radius:50%;'
            f'background:{c};"></span>{label}</span>'
        )

    legend = (
        f'<div style="display:flex;gap:22px;margin-top:12px;'
        f'font-size:.82rem;flex-wrap:wrap;">'
        + dot(GREEN,   "At/above benchmark")
        + dot(YELLOW,  "Near benchmark")
        + dot(RED,     "Below benchmark")
        + dot(NEUTRAL, "Insufficient data")
        + "</div>"
    )

    # ── Trending arrows for the summary bar ─────────────────────────────────
    def trend_badge(ratios_list: list[dict]) -> str:
        """Return a mini trend summary for the page header."""
        if len(ratios_list) < 2:
            return ""
        improving, declining, stable = 0, 0, 0
        for rk, info in RATIOS.items():
            vals = [r.get(rk) for r in ratios_list if r.get(rk) is not None]
            if len(vals) < 2:
                stable += 1
                continue
            diff = vals[-1] - vals[0]
            if info["direction"] == "higher":
                if diff > 0.001:
                    improving += 1
                elif diff < -0.001:
                    declining += 1
                else:
                    stable += 1
            elif info["direction"] == "lower":
                if diff < -0.001:
                    improving += 1
                elif diff > 0.001:
                    declining += 1
                else:
                    stable += 1
            else:
                stable += 1
        return (
            f'<span style="font-size:.82rem;opacity:.8;">'
            f'↑ {improving} improving &nbsp; ↓ {declining} declining &nbsp; '
            f'→ {stable} stable</span>'
        )

    analysis_html = _md(analysis)
    today_str     = date.today().strftime("%B %d, %Y")
    q_range       = f"{quarters[0]} – {quarters[-1]}"
    trend_info    = trend_badge(ratios)
    safe_name     = re.sub(r"[^\w\s-]", "", cu_name).strip().replace(" ", "_")
    export_artifacts = export_artifacts or {
        "fileBase": safe_name or "credit_union_dashboard",
        "markdown": "",
        "csv": "",
        "json": "",
    }
    export_json = json.dumps(export_artifacts, ensure_ascii=False).replace("</", "<\\/")

    export_toolbar = f"""
  <div class="export-toolbar" aria-label="Dashboard export controls">
    <div class="export-menu">
      <button type="button" id="export-button" class="export-button"
              aria-haspopup="true" aria-expanded="false"
              onclick="toggleExportMenu(event)">Export</button>
      <div id="export-options" class="export-options" role="menu" aria-label="Export formats">
        <button type="button" role="menuitem" onclick="exportDashboard('pdf')">PDF</button>
        <button type="button" role="menuitem" onclick="exportDashboard('html')">HTML</button>
        <button type="button" role="menuitem" onclick="exportDashboard('md')">Markdown (.md)</button>
        <button type="button" role="menuitem" onclick="exportDashboard('json')">JSON</button>
        <button type="button" role="menuitem" onclick="exportDashboard('csv')">CSV</button>
      </div>
    </div>
  </div>"""

    # ── Metadata bar ────────────────────────────────────────────────────────
    if cu_meta:
        def meta_chip(icon: str, label: str, value: str) -> str:
            return (
                f'<span style="display:inline-flex;align-items:center;gap:5px;'
                f'background:rgba(255,255,255,.12);border-radius:6px;'
                f'padding:4px 10px;font-size:.82rem;">'
                f'<span style="opacity:.7;">{icon}</span>'
                f'<span style="opacity:.7;">{label}:</span>'
                f'<strong>{value}</strong></span>'
            )
        chips = [
            meta_chip("📍", "Location", f"{cu_meta.get('city','')} {cu_meta.get('state','')}".strip()),
            meta_chip("🏛", "Charter", cu_meta.get("cu_type", "")),
            meta_chip("👥", "Field of Membership", cu_meta.get("fom", "")),
        ]
        if cu_meta.get("members") is not None:
            chips.append(meta_chip("👥", "Members", f"{cu_meta['members']:,}"))
        if cu_meta.get("num_branches") is not None:
            chips.append(meta_chip("🏢", "Branches", str(cu_meta["num_branches"])))
        if cu_meta.get("ceo_name"):
            chips.append(meta_chip("👤", "CEO", cu_meta["ceo_name"]))
        if cu_meta.get("year_opened"):
            chips.append(meta_chip("📅", "Est.", cu_meta["year_opened"]))
        if cu_meta.get("low_income"):
            chips.append(
                '<span style="display:inline-flex;align-items:center;gap:5px;'
                'background:rgba(26,188,156,.3);border-radius:6px;'
                'padding:4px 10px;font-size:.82rem;"><strong>✓ Low-Income Designated</strong></span>'
            )
        meta_bar = (
            '<div style="display:flex;flex-wrap:wrap;gap:8px;margin-top:14px;">'
            + "".join(chips)
            + "</div>"
        )
    else:
        meta_bar = ""

    # ── Total assets stat cards ──────────────────────────────────────────────
    def fmt_assets(v: Optional[float]) -> str:
        if v is None:
            return "N/A"
        if v >= 1_000_000_000:
            return f"${v / 1_000_000_000:.2f}B"
        if v >= 1_000_000:
            return f"${v / 1_000_000:.1f}M"
        return f"${v:,.0f}"

    asset_cards = ""
    for i, r in enumerate(ratios):
        ta = r.get("_total_assets")
        prev_ta = ratios[i - 1].get("_total_assets") if i > 0 else None
        if ta and prev_ta:
            chg = (ta - prev_ta) / prev_ta
            chg_html = (
                f'<span style="font-size:.78rem;margin-left:6px;'
                f'color:{"#27ae60" if chg >= 0 else "#e74c3c"};">'
                f'{"▲" if chg >= 0 else "▼"} {abs(chg):.1%} QoQ</span>'
            )
        else:
            chg_html = ""
        asset_cards += f"""
        <div style="flex:1;background:white;border-radius:10px;padding:18px 24px;
                    box-shadow:0 2px 10px rgba(0,0,0,.06);text-align:center;">
          <div style="font-size:.78rem;color:#6c7a89;font-weight:500;
                      text-transform:uppercase;letter-spacing:.05em;margin-bottom:6px;">
            Total Assets · {r["quarter"]}
          </div>
          <div style="font-size:1.75rem;font-weight:700;color:{DARK};">
            {fmt_assets(ta)}{chg_html}
          </div>
        </div>"""

    # ── Assemble full HTML ───────────────────────────────────────────────────
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>{cu_name} — NCUA 5300 Dashboard</title>
  <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap"
        rel="stylesheet">
  <style>
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body   {{ font-family: Inter, 'Helvetica Neue', Arial, sans-serif;
              background: #eef1f5; color: {DARK}; line-height: 1.6; }}
    a      {{ color: #2980b9; text-decoration: none; }}
    a:hover{{ text-decoration: underline; }}

    .header {{
      background: linear-gradient(135deg, {DARK} 0%, #3d566e 100%);
      color: white; padding: 28px 44px; border-bottom: 4px solid #1abc9c;
    }}
    .header h1  {{ font-size: 1.9rem; font-weight: 700; letter-spacing: -.4px; }}
    .header .sub {{ font-size: .88rem; opacity: .72; margin-top: 5px;
                    display: flex; align-items: center; gap: 14px; flex-wrap: wrap; }}
    .header .sub span {{ opacity: 1; }}

    .wrapper {{ max-width: 1480px; margin: 0 auto; padding: 32px 26px; }}

    .export-toolbar {{
      position: sticky; top: 0; z-index: 10;
      display: flex; align-items: center; justify-content: flex-end;
      padding: 10px 26px; background: rgba(238,241,245,.96);
      border-bottom: 1px solid #dfe5ec; backdrop-filter: blur(8px);
    }}
    .export-menu {{ position: relative; }}
    .export-button {{
      border: 1px solid {DARK}; border-radius: 6px; background: {DARK};
      color: white; font: 700 .84rem Inter, 'Helvetica Neue', Arial, sans-serif;
      padding: 8px 13px; cursor: pointer;
    }}
    .export-button::after {{
      content: "▾"; display: inline-block; margin-left: 8px; font-size: .75rem;
    }}
    .export-button:hover, .export-button[aria-expanded="true"] {{ background: #1f2f3f; }}
    .export-options {{
      display: none; position: absolute; right: 0; top: calc(100% + 8px);
      min-width: 168px; background: white; border: 1px solid #c9d3df;
      border-radius: 8px; box-shadow: 0 12px 28px rgba(31,47,63,.18);
      padding: 5px; z-index: 20;
    }}
    .export-options.open {{ display: block; }}
    .export-options button {{
      display: block; width: 100%; border: 0; background: transparent;
      color: {DARK}; font: 600 .84rem Inter, 'Helvetica Neue', Arial, sans-serif;
      text-align: left; padding: 8px 10px; border-radius: 5px; cursor: pointer;
    }}
    .export-options button:hover, .export-options button:focus {{
      background: #eef3f8; outline: none;
    }}

    .card {{
      background: white; border-radius: 12px; padding: 28px 32px;
      box-shadow: 0 2px 12px rgba(0,0,0,.06); margin-bottom: 26px;
    }}
    .card h2 {{
      font-size: 1.05rem; font-weight: 600; color: {DARK};
      border-bottom: 2px solid #eef1f5; padding-bottom: 10px;
      margin-bottom: 20px; display: flex; align-items: center; gap: 8px;
    }}
    .card h2 .badge {{
      font-size: .72rem; font-weight: 500; background: #eef1f5;
      color: #6c7a89; padding: 2px 8px; border-radius: 99px;
    }}

    /* Table */
    table  {{ width: 100%; border-collapse: collapse; font-size: .88rem; }}
    thead th {{
      font-weight: 600; font-size: .82rem;
      text-align: left; border-bottom: 2px solid #dee2e6;
    }}
    tbody td {{ border-bottom: 1px solid #f0f3f6; }}
    tbody tr:last-child td {{ border-bottom: none; }}
    tbody tr:hover td {{ background: #f8f9fa !important; }}

    /* Analysis prose */
    .analysis {{ font-size: .93rem; color: #34495e; line-height: 1.8; }}
    .analysis p     {{ margin-bottom: .9rem; }}
    .analysis h3    {{ font-size: 1rem; font-weight: 600; color: {DARK};
                        margin: 1.2rem 0 .4rem; }}
    .analysis h4    {{ font-size: .95rem; font-weight: 600; color: {DARK};
                        margin: 1rem 0 .3rem; }}
    .analysis ul    {{ margin: .3rem 0 .8rem 1.4rem; }}
    .analysis li    {{ margin-bottom: .3rem; }}
    .analysis strong {{ color: {DARK}; }}

    /* Data source note */
    .source-note {{
      font-size: .78rem; color: #95a5a6; margin-top: 10px;
    }}

    footer {{
      text-align: center; padding: 22px;
      font-size: .78rem; color: #95a5a6;
    }}
    footer a {{ color: #95a5a6; }}

    @media print {{
      body {{ background: white; }}
      .export-toolbar {{ display: none !important; }}
      .wrapper {{ max-width: none; padding: 18px; }}
      .card {{ box-shadow: none; break-inside: avoid; page-break-inside: avoid; }}
      .header {{ print-color-adjust: exact; -webkit-print-color-adjust: exact; }}
    }}
  </style>
</head>
<body>

<header class="header">
  <h1>{cu_name}{f" <span style='font-size:.6em;font-weight:400;opacity:.65;'>Charter #{cu_meta['cu_number']}</span>" if cu_meta and cu_meta.get("cu_number") else ""}</h1>
  <div class="sub">
    <span>NCUA 5300 Call Report Financial Dashboard</span>
    <span>·</span>
    <span>{q_range}</span>
    <span>·</span>
    <span>Generated {today_str}</span>
    <span>·</span>
    {trend_info}
  </div>
  {meta_bar}
</header>

{export_toolbar}

<div class="wrapper">

  <!-- ── Upstart Recommendation Banner (drew3) ────────────────────────── -->
  {upstart_top_html}

  <!-- ── Total Assets ────────────────────────────────────────────────── -->
  <div style="display:flex;gap:16px;margin-bottom:26px;flex-wrap:wrap;">
    {asset_cards}
  </div>

  <!-- ── Ratio Summary Table ─────────────────────────────────────────── -->
  <div class="card">
    <h2>
      Key Ratio Summary
      <span class="badge">{len(ratios)} quarters</span>
    </h2>
    <table>
      <thead>
        <tr>
          <th style="padding:9px 14px;">Metric</th>
          <th style="padding:9px 14px;">Description</th>
          <th style="padding:9px 14px;text-align:center;">Benchmark</th>
          {th_qs}
        </tr>
      </thead>
      <tbody>
        {tbody}
      </tbody>
    </table>
    {legend}
    <p class="source-note">
      Benchmarks reflect NCUA peer averages and regulatory thresholds.
      Net Worth Ratio benchmark (7%) is the NCUA "well-capitalized" minimum.
      Growth figures are annualised quarter-over-quarter changes.
    </p>
  </div>

  <!-- ── Shares & Deposits ─────────────────────────────────────────── -->
  {shares_html}

  <!-- ── Loan Portfolio ────────────────────────────────────────────── -->
  {loans_html}

  <!-- ── Investment Portfolio ──────────────────────────────────────── -->
  {investments_html}

  <!-- ── Asset Class Breakdown ─────────────────────────────────────── -->
  {asset_class_html}

  <!-- ── Trend Charts ────────────────────────────────────────────────── -->
  <div class="card">
    <h2>Trend Charts <span class="badge">3-quarter view</span></h2>
    {chart_html}
    <p class="source-note" style="margin-top:8px;">
      Dashed lines = industry benchmarks.  Dotted overlay = trend direction.
      Bar colour: <span style="color:{GREEN};font-weight:600;">green</span> = at/above benchmark,
      <span style="color:{YELLOW};font-weight:600;">yellow</span> = near,
      <span style="color:{RED};font-weight:600;">red</span> = below.
    </p>
  </div>

  <!-- ── AI Analysis ─────────────────────────────────────────────────── -->
  <div class="card">
    <h2>AI Analysis <span class="badge">ChatGPT · gpt-4o</span></h2>
    <div class="analysis">{analysis_html}</div>
  </div>

  <!-- ── HMDA Mortgage Originations ────────────────────────────────────── -->
  {hmda_html}

  <!-- ── Upstart Rationale Card (drew3) ───────────────────────────────── -->
  {upstart_bottom_html}

  <!-- ── Sales Conversation Guide (drew3.2) ───────────────────────────── -->
  {sales_questions_html}

</div>

<footer>
  Data source: <a href="https://ncua.gov/analysis/credit-union-corporate-call-report-data"
  target="_blank">NCUA 5300 Call Report Bulk Data</a>
  &nbsp;|&nbsp;
  Mortgage data: <a href="https://ffiec.cfpb.gov/data-browser/" target="_blank">FFIEC HMDA Data Browser</a>
  &nbsp;|&nbsp;
  AI analysis: OpenAI GPT-4o
  &nbsp;|&nbsp;
  For informational purposes only — not investment or regulatory advice.
</footer>

<script>
  window.__DASHBOARD_EXPORTS__ = {export_json};

  function downloadDashboardAsset(filename, mimeType, contents) {{
    const blob = new Blob([contents], {{ type: mimeType }});
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(url);
  }}

  function dashboardHtmlExport() {{
    const clone = document.documentElement.cloneNode(true);
    const toolbar = clone.querySelector('.export-toolbar');
    if (toolbar) toolbar.remove();
    return '<!DOCTYPE html>\\n' + clone.outerHTML;
  }}

  function closeExportMenu() {{
    const menu = document.getElementById('export-options');
    const button = document.getElementById('export-button');
    if (menu) menu.classList.remove('open');
    if (button) button.setAttribute('aria-expanded', 'false');
  }}

  function toggleExportMenu(event) {{
    event.stopPropagation();
    const menu = document.getElementById('export-options');
    const button = document.getElementById('export-button');
    const isOpen = menu.classList.toggle('open');
    button.setAttribute('aria-expanded', isOpen ? 'true' : 'false');
  }}

  function exportDashboard(format) {{
    closeExportMenu();
    const data = window.__DASHBOARD_EXPORTS__ || {{}};
    const base = data.fileBase || 'credit_union_dashboard';

    if (format === 'pdf') {{
      window.print();
      return;
    }}
    if (format === 'html') {{
      downloadDashboardAsset(base + '_dashboard.html', 'text/html;charset=utf-8', dashboardHtmlExport());
      return;
    }}
    if (format === 'md') {{
      downloadDashboardAsset(base + '_dashboard.md', 'text/markdown;charset=utf-8', data.markdown || '');
      return;
    }}
    if (format === 'json') {{
      downloadDashboardAsset(base + '_dashboard.json', 'application/json;charset=utf-8', data.json || '{{}}');
      return;
    }}
    if (format === 'csv') {{
      downloadDashboardAsset(base + '_dashboard.csv', 'text/csv;charset=utf-8', data.csv || '');
    }}
  }}

  document.addEventListener('click', closeExportMenu);
  document.addEventListener('keydown', function(event) {{
    if (event.key === 'Escape') closeExportMenu();
  }});
</script>

</body>
</html>"""

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"  ✓ Dashboard → {out_path}")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print("║  NCUA 5300 Call Report  ·  Financial Dashboard Builder   ║")
    print("║           Drew3 · + HMDA + Upstart Recommendation        ║")
    print("╚══════════════════════════════════════════════════════════╝")
    print()

    if len(sys.argv) > 1:
        query = " ".join(sys.argv[1:]).strip()
        print(f"Credit union name: {query}")
    else:
        query = input("Credit union name: ").strip()
    if not query:
        sys.exit("No name entered — exiting.")

    print(f"\nData cache: {CACHE_DIR}\n")

    # ── Step 1: Locate the credit union ─────────────────────────────────────
    print("─" * 58)
    print("[1/5]  Locating credit union in NCUA bulk data…")
    print("─" * 58)

    cu_info: Optional[dict]  = None
    zip_cache: dict[tuple, zipfile.ZipFile] = {}

    # Probe quarters from newest to oldest, stop once 3 are collected.
    # No fixed lag — fetch_zip returns None if a quarter isn't published yet.
    for year, month in candidate_quarters(6):
        if len(zip_cache) == 3:
            break
        print(f"  Checking {ql(year, month)}…")
        zf = fetch_zip(year, month)
        if zf is None:
            continue
        zip_cache[(year, month)] = zf
        if cu_info is None:
            hit = find_cu_in_zip(zf, query)
            if hit:
                cu_info = hit
                print(f"  ✓ {cu_info['cu_name']}  (Charter #{cu_info['cu_number']})")

    if cu_info is None:
        print(
            f"\n✗ No match for '{query}' in NCUA data.\n\n"
            "  Suggestions:\n"
            "  • Use a shorter/partial name  "
            "(e.g. 'Navy Federal' not 'Navy Federal Credit Union')\n"
            "  • Verify the charter number at  https://mapping.ncua.gov/ResearchCreditUnion\n"
            "  • Check that cached zips are not corrupted  (delete ~/.cache/ncua_5300/)\n"
        )
        sys.exit(1)

    charter = cu_info["cu_number"]
    name    = cu_info["cu_name"]

    # Build ordered quarter list from what actually downloaded (newest first).
    quarters = sorted(zip_cache.keys(), key=lambda t: (t[0], t[1]), reverse=True)
    ya_year, ya_month = quarters[0][0] - 1, quarters[0][1]
    print(
        f"  Selected quarters : {', '.join(ql(y, m) for y, m in quarters)}"
        f"\n  Year-ago quarter  : {ql(ya_year, ya_month)} (for shares YoY)\n"
    )

    # ── Step 2: Extract financials for all 3 quarters ───────────────────────
    print()
    print("─" * 58)
    print(f"[2/5]  Extracting 5300 data for {name}…")
    print("─" * 58)

    raw: list[dict] = []
    for year, month in quarters:
        zf = zip_cache.get((year, month)) or fetch_zip(year, month)
        if zf is None:
            print(f"  ✗ {ql(year, month)} — download failed, skipping")
            continue
        zip_cache[(year, month)] = zf
        # Charter numbers can change between quarters; resolve by name each time.
        q_charter = _resolve_charter(zf, name) or charter
        rec = extract_financials(zf, q_charter, year, month)
        if rec:
            raw.append(rec)
            print(f"  ✓ {ql(year, month)} — row extracted")
        else:
            print(f"  ✗ {ql(year, month)} — data row not found")

    if not raw:
        sys.exit("\n✗ No financial data retrieved — cannot build dashboard.")

    # Sort oldest → newest so trend lines read left-to-right
    raw.sort(key=lambda r: (r["_year"], r["_month"]))

    # ── Step 3: Compute ratios ───────────────────────────────────────────────
    print()
    print("─" * 58)
    print("[3/5]  Computing financial ratios…")
    print("─" * 58)

    ratio_rows: list[dict] = []
    for i, rec in enumerate(raw):
        prev = raw[i - 1] if i > 0 else None
        r = calc_ratios(rec, prev)
        ratio_rows.append(r)

        nwr = r.get("net_worth_ratio")
        roa = r.get("roa")
        lts = r.get("loan_to_share")
        dq  = r.get("delinquency_ratio")
        print(
            f"  {r['quarter']}  │  "
            f"NW {fv('net_worth_ratio', nwr)}  │  "
            f"ROA {fv('roa', roa)}  │  "
            f"L/S {fv('loan_to_share', lts)}  │  "
            f"DQ {fv('delinquency_ratio', dq)}"
        )

    # ── Step 4: AI analysis + dashboard ─────────────────────────────────────
    print()
    print("─" * 58)
    print("[4/5]  Generating AI analysis…")
    print("─" * 58)

    analysis = gpt_analysis(name, ratio_rows)

    # DREW3: Upstart recommendation ──────────────────────────────────────────
    cur_loans_for_rec  = extract_loans(raw[-1]) if raw else None
    cur_losses_for_rec = extract_loan_losses(raw[-1]) if raw else None
    cur_rates_for_rec  = extract_loan_rates(raw[-1]) if raw else None
    upstart_rec        = compute_upstart_recommendation(
        name, ratio_rows, cur_loans_for_rec, cur_losses_for_rec, cur_rates_for_rec
    )
    upstart_top_html  = build_upstart_recommendation_html(upstart_rec, name)
    upstart_bot_html  = build_upstart_rationale_html(upstart_rec)
    sales_q_html      = build_sales_questions_html(upstart_rec, name)
    print(
        f"  Upstart recommendation: {upstart_rec['overall']} "
        f"[{upstart_rec['confidence']} confidence]"
    )
    # ── end DREW3 addition ───────────────────────────────────────────────────

    # ── Step 5: HMDA mortgage origination data ───────────────────────────────
    print()
    print("─" * 58)
    print("[5/5]  Fetching HMDA mortgage origination data…")
    print("─" * 58)

    hmda_html = ""
    hmda_data: dict = {}
    cu_state  = cu_info.get("state", "") if cu_info else ""
    print("  Looking up LEI in GLEIF registry…")
    lei = lookup_lei(name)
    if lei:
        print(f"  ✓ LEI: {lei}")
        print("  Querying FFIEC HMDA Data Browser…")
        hmda_data = fetch_hmda_data(lei, cu_state)
        if hmda_data.get("found"):
            tc  = hmda_data["total_count"]
            ts  = hmda_data["total_sum"]
            yr  = hmda_data["year"]
            vol = (
                f"${ts/1_000_000:.1f}M" if ts >= 1_000_000
                else f"${ts/1_000:.0f}K"
            )
            print(f"  ✓ HMDA {yr}: {tc:,} originations, {vol} volume")
            hmda_html = build_hmda_section(hmda_data)
        else:
            print(f"  ✗ No HMDA origination data found for LEI {lei}")
    else:
        print("  ✗ LEI not found in GLEIF — skipping HMDA section")

    # ── Shares & Deposits / Loans / Investments / Asset Class sections ─────
    shares_html      = ""
    loans_html       = ""
    investments_html = ""
    asset_class_html = ""
    export_data: dict = {"hmda": hmda_data}
    if len(raw) >= 2:
        # Use the same 3 quarters as the key ratio summary (oldest → newest)
        cur_rec   = raw[-1]
        prior_rec = raw[-2]
        old_rec   = raw[0]   # oldest fetched quarter (matches trend table leftmost column)

        # Add member count from latest call report into cu_info for the header
        if cu_info is not None:
            m = _get(cur_rec, "members")
            if m and m > 0:
                cu_info["members"] = int(m)

        old_label   = ql(old_rec["_year"],   old_rec["_month"])
        prior_label = ql(prior_rec["_year"], prior_rec["_month"])
        cur_label   = ql(cur_rec["_year"],   cur_rec["_month"])
        export_data["labels"] = {
            "old": old_label,
            "prior": prior_label,
            "current": cur_label,
        }

        cur_shares   = extract_shares(cur_rec)
        prior_shares = extract_shares(prior_rec)
        old_shares   = extract_shares(old_rec)
        export_data["shares"] = {
            "old": old_shares,
            "prior": prior_shares,
            "current": cur_shares,
        }

        shares_html = build_shares_table(
            ya_label    = old_label,
            prior_label = prior_label,
            cur_label   = cur_label,
            ya_shares   = old_shares,
            prior_shares= prior_shares,
            cur_shares  = cur_shares,
        )

        cur_loans   = extract_loans(cur_rec)
        prior_loans = extract_loans(prior_rec)
        old_loans   = extract_loans(old_rec)
        export_data["loans"] = {
            "old": old_loans,
            "prior": prior_loans,
            "current": cur_loans,
        }

        cur_rates           = extract_loan_rates(cur_rec)
        cur_portfolio_yield = compute_portfolio_yield(cur_rec)
        cur_losses          = extract_loan_losses(cur_rec)
        cur_portfolio_nco   = compute_portfolio_nco(cur_rec)
        export_data["loan_rates"] = cur_rates
        export_data["loan_losses"] = cur_losses
        export_data["portfolio_yield"] = cur_portfolio_yield
        export_data["portfolio_nco"] = cur_portfolio_nco

        loans_html = build_loans_table(
            ya_label            = old_label,
            prior_label         = prior_label,
            cur_label           = cur_label,
            ya_loans            = old_loans,
            prior_loans         = prior_loans,
            cur_loans           = cur_loans,
            cur_rates           = cur_rates,
            cur_portfolio_yield = cur_portfolio_yield,
            cur_losses          = cur_losses,
            cur_portfolio_nco   = cur_portfolio_nco,
        )

        cur_inv    = extract_investments(cur_rec)
        prior_inv  = extract_investments(prior_rec)
        old_inv    = extract_investments(old_rec)
        cur_inv_yield = compute_investment_yield(cur_rec)
        export_data["investments"] = {
            "old": old_inv,
            "prior": prior_inv,
            "current": cur_inv,
        }
        export_data["investment_yield"] = cur_inv_yield

        investments_html = build_investments_table(
            ya_label    = old_label,
            prior_label = prior_label,
            cur_label   = cur_label,
            ya_inv      = old_inv,
            prior_inv   = prior_inv,
            cur_inv     = cur_inv,
            cur_yield   = cur_inv_yield,
        )

        cur_ac   = extract_asset_classes(cur_loans)
        prior_ac = extract_asset_classes(prior_loans)
        old_ac   = extract_asset_classes(old_loans)
        export_data["asset_classes"] = {
            "old": old_ac,
            "prior": prior_ac,
            "current": cur_ac,
        }

        asset_class_html = build_asset_class_section(
            ya_label    = old_label,
            prior_label = prior_label,
            cur_label   = cur_label,
            ya_ac       = old_ac,
            prior_ac    = prior_ac,
            cur_ac      = cur_ac,
        )

    safe_fn  = re.sub(r"[^\w\s-]", "", name).strip().replace(" ", "_")
    out_file = f"{safe_fn}_Drew3_dashboard.html"
    export_artifacts = build_export_artifacts(
        name,
        ratio_rows,
        analysis,
        cu_info,
        export_data,
        upstart_rec,
        sales_q_html,
    )
    build_dashboard(name, ratio_rows, analysis, out_file, cu_meta=cu_info,
                    shares_html=shares_html, loans_html=loans_html,
                    investments_html=investments_html,
                    asset_class_html=asset_class_html, hmda_html=hmda_html,
                    upstart_top_html=upstart_top_html,       # DREW3
                    upstart_bottom_html=upstart_bot_html,    # DREW3
                    sales_questions_html=sales_q_html,       # DREW3.2
                    export_artifacts=export_artifacts)

    abs_path = os.path.abspath(out_file)
    webbrowser.open(f"file://{abs_path}")
    print()
    print("─" * 58)
    print("  Done!  Dashboard saved:")
    print(f"    {abs_path}")
    print("─" * 58)
    print()

    # Tidy up zip handles
    for zf in zip_cache.values():
        try:
            zf.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
