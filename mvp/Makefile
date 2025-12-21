flags/create:
	mkdir -p flags/
	touch flags/create

flags/deploy: flags/create
	databricks bundle deploy
	touch flags/deploy

flags/ingest_to_bronze: flags/deploy
	databricks bundle run ingest_to_bronze
	touch flags/ingest_to_bronze


clean:
	rm -rf flags/