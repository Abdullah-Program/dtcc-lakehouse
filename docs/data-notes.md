# DTCC PPD data notes

Source: CFTC cumulative RATES file `CFTC_CUMULATIVE_RATES_2026_10_02.zip` (26,840 rows, 110 columns).

## Observed (from ingestion/explore_actions.py)
- Action type counts: NEWT 21349, MODI 2413, CORR 1487, TERM 1120, EROR 369, REVI 102.
- Original Dissemination Identifier is empty only for NEWT.
- Dissemination Identifier is unique within the file.
- ~32% of non-NEWT rows (1776 of 5491) point to an Original ID not in the same file.
- Event type is empty for exactly as many rows as CORR+EROR+REVI (1958); to be verified row by row.
- At least one TERM row has Original ID equal to its own Dissemination ID.

## Open questions
- Official meaning of each Action type and of Amendment indicator.
- Does Original ID point to the previous message or to the root trade?
- Which column gives correct ordering (CSV has no dissemination timestamp).
## Findings from explore_chains.py (same RATES file, 2026-10-02)
- Same-file Original IDs point to NEWT rows in 3681 of 3682 cases (1 CORR -> CORR). No MODI/TERM targets.
- One original can have up to 21 child messages; 3042 of 3693 originals have exactly one.
- Child Dissemination ID is larger than its target's ID in 3682 of 3682 cases.
- Event type is empty for every CORR/EROR/REVI row and filled for every NEWT/MODI/TERM row.
- 33 self-referencing rows (TERM 28, CORR 2, MODI 3).
- Dissemination ID is always 19 digits; 551 Original IDs are shorter (8-10 digits) or 18 digits. Treat IDs as strings.
- Provisional design: trade_key = own ID for NEWT, Original ID otherwise. Re-check on a month of data in Phase 1.
