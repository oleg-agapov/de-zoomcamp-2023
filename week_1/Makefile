.PHONY: jupyterlab
jupyterlab:
	docker run -it -p 8888:8888 -v $(PWD):/home/jovyan jupyter/datascience-notebook:python-3.9.13


.PHONY: postgres
postgres:
	docker run -it --rm \
		-e POSTGRES_USER=root \
		-e POSTGRES_PASSWORD=root \
		-e POSTGRES_DB=ny_taxi \
		-v $(PWD)/pg_data:/var/lib/postgresql/data \
		-p 5431:5432 \
		--network=my-network \
		--name pg_database \
		postgres:13


.PHONY: jupyter
jupyter:	
	docker build -t my-python .
	docker run -it --rm \
		-v $(PWD):/app \
		-p 8888:8888 \
		--network=my-network \
		--entrypoint=jupyter \
		my-python notebook --allow-root --ip 0.0.0.0


.PHONY: pgcli
pgcli:	
	docker build -t my-python .
	docker run -it --rm \
		-v $(PWD):/app \
		--network=my-network \
		--entrypoint=pgcli \
		my-python -h pg_database -p 5432 -u root -d ny_taxi


.PHONY: ingest
ingest:	
	docker build -t my-python .
	docker run -it --rm \
		-v $(PWD):/app \
		--network=my-network \
		--entrypoint=python \
		my-python ingest.py
