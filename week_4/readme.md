# Week 4 – Analytics engineering

```bash
docker pull ghcr.io/dbt-labs/dbt-bigquery:1.4.0

# dbt init
docker run -it --rm -v $(pwd):/usr/app/dbt ghcr.io/dbt-labs/dbt-bigquery:1.4.0 init

# open bash
docker run -it --rm -v $(pwd):/usr/app/dbt --entrypoint bash ghcr.io/dbt-labs/dbt-bigquery:1.4.0
```
