# Prefilter Pipeline Notes

This document describes the prefilter stage of the foundation opportunity scraper.

## Purpose

The goal of the prefilter is to reduce obvious non-opportunity rows before downstream
classification and matching. This minimizes cost and noise while preserving recall.

## Input

- Extracted opportunity rows produced by the LLM extraction step
- Rows may include:
  - real funding opportunities
  - recognition-only awards
  - informational program pages
  - retrospective summaries

## Output

- Excel file: `foundations_and_opportunities_PREFILTERED.xlsx`
- Primary sheet: `Opportunities_prefiltered`
- Additional sheets include dropped rows with reasons

## Common drop reasons

Examples of reasons used during prefiltering:

- recognition / honor with no funding
- informational page with no application mechanism
- page describes program but no money or awards
- URL or name patterns strongly associated with non-opportunities
- announcements of past awards without an open call

## Design philosophy

- Conservative: prefer keeping borderline rows rather than dropping them
- Deterministic: rule-based decisions with explicit reasons
- Auditable: every dropped row has a documented reason

Rows that survive the prefilter are intentionally heterogeneous and are handled
by the downstream LLM classifier.
