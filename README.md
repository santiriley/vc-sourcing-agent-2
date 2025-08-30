# VC Sourcing Agent

This repository contains utilities for scoring startup opportunities.

## Scoring configuration

The scoring heuristic is controlled by the `config.yaml` file.  Each weight
adjusts the score when certain signals are present:

- `central_america_presence` – added when the startup lists a location in one of
  Belize, Costa Rica, El Salvador, Guatemala, Honduras, Nicaragua or Panama.
- `south_of_mexico_presence` – added when the location is in South America.
- `fintech_keyword_penalty` – subtracted if the description contains fintech
  terms such as "fintech", "banking" or "payments".
- `female_founder_boost` – added when any founder is identified as female.

Modify these weights in `config.yaml` to tune how opportunities are scored.
