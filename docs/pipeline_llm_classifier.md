# LLM Opportunity Classifier Pipeline

This document describes the LLM-based classification stage applied after prefiltering.

## Goal

Identify *real, prospective funding opportunities that provide money* while minimizing
false positives.

## Input

- Excel file: `foundations_and_opportunities_PREFILTERED.xlsx`
- Sheet: `Opportunities_prefiltered`
- Approximate size: ~3,700 rows

## Output schema

Each row is augmented with:

- `is_real_funding`: `yes | no | unclear`
- `reason`: one-sentence audit trail
- `confidence`: currently forced to `low` (conservative default)

## Classification policy

- **YES**
  - Explicit grants, fellowships, stipends, travel funding, salary support
- **NO**
  - Recognition-only awards
  - Informational or program pages with no application
  - Lists of past recipients or funded projects
- **UNCLEAR**
  - Ambiguous cases
  - Travel awards with unclear funding language
  - Pages mixing opportunity descriptions with retrospective content
  - Aggregator or umbrella pages

Preference is given to `unclear` over `yes` when funding is not explicit.

## Resumability and audit trail

- Classification is resumable via a checkpoint CSV written every N rows
- Each row includes a human-readable reason for the classification

## Example run (Jan 2026)

Initial LLM pass:
- yes: 2551
- unclear: 899
- no: 267

Rule-based demotion experiments were evaluated and intentionally abandoned
to avoid introducing false negatives. The LLM output is treated as authoritative.

## Design principle

The classifier is intentionally conservative. The `unclear` category is a
designed outcome to support human review and downstream curation.
