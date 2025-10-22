# ------------------------
# Variables
# ------------------------
PYTHON_VERSION ?= 3.11.7       # Python version for pyenv
VENV_NAME ?= load-env
REQ_FILE = ../extract/requirements.txt
SCRIPT = ../extract/src/upload_to_s3.py

S3_ENDPOINT = http://localhost:9000
ACCESS_KEY = admin
SECRET_KEY = password
REGION = eu-west-2

SPARK_CONTAINER = spark-iceberg
SPARK_JOB_NAME_BASICS = /app/src/load_to_iceberg_name_basics.py
SPARK_JOB_TITLE_AKAS = /app/src/load_to_iceberg_title_akas.py
SPARK_JOB_TITLE_BASICS = /app/src/load_to_iceberg_title_basics.py
SPARK_JOB_TITLE_CREW = /app/src/load_to_iceberg_title_crew.py
SPARK_JOB_TITLE_EPISODE = /app/src/load_to_iceberg_title_episode.py
SPARK_JOB_TITLE_PRINCIPALS = /app/src/load_to_iceberg_title_principals.py
SPARK_JOB_TITLE_RATINGS = /app/src/load_to_iceberg_title_ratings.py

SPARK_JOBS = \
	$(SPARK_JOB_NAME_BASICS) \
	$(SPARK_JOB_TITLE_AKAS) \
	$(SPARK_JOB_TITLE_BASICS) \
	$(SPARK_JOB_TITLE_CREW) \
	$(SPARK_JOB_TITLE_EPISODE) \
	$(SPARK_JOB_TITLE_PRINCIPALS) \
	$(SPARK_JOB_TITLE_RATINGS)

# ------------------------
# Targets
# ------------------------

.PHONY: up extract load clean

# Start MinIO container in detached mode
up:
	docker compose up -d

# Setup local Python env and run the script
extract:
	@echo "Setting up Python environment..."
	# Install Python and create virtualenv if needed
	pyenv install -s $(PYTHON_VERSION)
	pyenv virtualenv -f $(PYTHON_VERSION) $(VENV_NAME) || true
	# Activate the virtualenv explicitly and run everything in one shell
	. $$(pyenv root)/versions/$(VENV_NAME)/bin/activate && \
	pip install --upgrade pip && \
	pip install -r $(REQ_FILE) && \
	S3_ENDPOINT=$(S3_ENDPOINT) \
	ACCESS_KEY=$(ACCESS_KEY) \
	SECRET_KEY=$(SECRET_KEY) \
	REGION=$(REGION) \
	python $(SCRIPT)


# Target to load all jobs sequentially
load:
	@for job in $(SPARK_JOBS); do \
		echo "=========================================="; \
		echo "Running Spark job: $$job"; \
		echo "=========================================="; \
		docker exec -it $(SPARK_CONTAINER) \
			spark-submit \
			--master local[*] \
			$$job; \
		if [ $$? -ne 0 ]; then \
			echo "Error running $$job"; \
			exit 1; \
		fi; \
	done




# Stop MinIO and remove containers/volumes
clean:
	docker compose down -v
	pyenv uninstall -f $(VENV_NAME) || true
	rm -f .python-version

