## Weather ETL

A data engineering project that extracts, transforms, and loads weather data using Dagster, a modern data orchestration platform. This ETL pipeline automates weather data collection and processing workflows.

### Definitions:

- `assets.py` - Pipeline's assets live here
- `ops.py` - Discord webhook notification when new data is materialized
- `resources.py` - API client and configurations live in here
