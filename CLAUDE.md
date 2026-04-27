# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

`drew3.1.py` is a CLI tool that generates a self-contained HTML financial dashboard for a credit union, used at Upstart for sales outreach and partner evaluation. It pulls public NCUA 5300 call-report data, computes financial ratios, runs an AI narrative, and scores the CU for Upstart product fit.

## Running the Script

```bash
# From anywhere (symlinked):
drew3.1

# Or directly:
python3 drew3.1.py
```

The script prompts interactively for a credit union name. Output is a self-contained HTML file (`{CU_NAME}_Drew3_dashboard.html`) that opens in the browser automatically.

## Dependencies

Third-party: `pandas`, `plotly`, `requests`, `openai` (optional — AI analysis degrades gracefully if `OPENAI_API_KEY` is not set).

## Five-Stage Pipeline

The `main()` function at the bottom orchestrates everything in order:

1. **Locate CU** — searches NCUA bulk ZIP files for a matching credit union by name, returning charter number and metadata.
2. **Extract financials** — downloads three most recent completed quarters of NCUA 5300 data. Each quarter merges FS220 (main) with supplemental schedules: FS220A, FS220B, FS220C, FS220H, FS220I, FS220L, FS220M, FS220P, FS220Q. ZIP files are cached in `~/.cache/ncua_5300/`.
3. **Compute ratios** — `calc_ratios()` derives ~14 financial metrics (ROA, NIM, efficiency ratio, loan-to-share, delinquency, charge-off, etc.) with YTD annualization via `ann_factor()`.
4. **AI analysis + Upstart recommendation** — sends ratio rows to GPT-4o for narrative; `compute_upstart_recommendation()` runs a scoring model across 15 signal categories (see below).
5. **HMDA data** — looks up the CU's LEI via GLEIF, then queries the FFIEC HMDA API for mortgage origination counts.

## Key Configuration Constants

All near the top of the file — change these to tune behavior:

```python
UPSTART_PERSONAL_YIELD   = 0.065   # net of losses & fees
UPSTART_AUTO_YIELD       = 0.055   # net of losses & fees
UPSTART_HELOC_YIELD      = 0.055   # net of losses & fees
DEALER_FEE_AUTO          = 0.010   # deducted from auto APR before comparing to Upstart yield
```

Ratio benchmarks live in the `RATIOS` dict (each entry has `benchmark` and `direction` keys).

Upstart scoring thresholds: `_CONF_HIGH_SCORE = 5`, `_CONF_MEDIUM_SCORE = 2`.

## NCUA Field Mappings

The `ACCT` dict maps semantic names (e.g., `"rate_new_auto"`) to lists of NCUA column name variants. Always add new fields here first — `_get(record, key)` resolves them automatically across format revisions.

Loan rates use **active** NCUA fields:
- ACCT_521 (credit card rate), ACCT_522 (other unsecured), ACCT_523 (new vehicle), ACCT_524 (used vehicle) — all in FS220 main file.
- ACCT_560 and ACCT_561 are **retired pre-1989** and return zero for all CUs — do not use them.

Charge-offs come from multiple supplemental schedules: credit card (FS220B), vehicle (FS220I), PAL/student (FS220H), most RE and other categories (FS220P).

## Upstart Recommendation Module

`compute_upstart_recommendation()` (lines ~367–900) evaluates 15 signal categories and returns a verdict, confidence level, product list, and rationale text. Key design points:

- Signals increment `score`; concerns can decrement it.
- `want_personal`, `want_auto`, `want_heloc` flags control which products are recommended.
- **Yield comparisons are net-of-losses on both sides** — when comparing existing CU loan yields to Upstart yields, deduct the CU's NCO rate from the gross loan rate first. Upstart yields (6.5%/5.5%) are already net of losses.
- Auto loans additionally deduct `DEALER_FEE_AUTO` before comparing to Upstart Auto yield.
- Section 14 handles per-product NCO analysis; section 15 handles dealer fee drag on auto.

The HTML output blocks are built by `build_upstart_recommendation_html()` and `build_upstart_rationale_html()`.

## Loan Portfolio Table

`build_loans_table()` renders the loan breakdown card with three columns of data. It accepts:
- `cur_rates` — from `extract_loan_rates()` — shows per-product APR
- `cur_losses` / `cur_portfolio_nco` — from `extract_loan_losses()` / `compute_portfolio_nco()` — shows annualised net charge-off % per loan type

`LOAN_RATE_KEY` and `LOAN_LOSS_KEY` map each loan category key to its NCUA field(s). Commercial RE and non-RE charge-offs are sums of multiple sub-category fields.
