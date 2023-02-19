.PHONY: dbt
dbt:
	docker run -it --rm \
    	--network host \
    	-v $(PWD):/usr/app/dbt \
		-v $(PWD)/.dbt:/root/.dbt \
    	--entrypoint bash \
    	ghcr.io/dbt-labs/dbt-bigquery:1.4.0
