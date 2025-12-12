# ------------------------
# Variables
# ------------------------
PYTHON_VERSION ?= 3.11.7       # Python version for pyenv
VENV_NAME ?= extract-env
REQ_FILE = requirements.txt

S3_ENDPOINT = http://localhost:9000
ACCESS_KEY = minioadmin
SECRET_KEY = minioadmin123
REGION = eu-west-2


RAW_NAME_BASIC = src/extract_name_basics_to_s3.py
RAW_TITLE_AKAS = src/extract_title_akas_to_s3.py
RAW_TITLE_BASICS = src/extract_title_basics_to_s3.py
RAW_TITLE_CREW = src/extract_title_crew_to_s3.py
RAW_TITLE_EPISODE = src/extract_title_episode_to_s3.py
RAW_TITLE_PRINCIPALS = src/extract_title_principals_to_s3.py
RAW_TITLE_RATINGS = src/extract_title_ratings_to_s3.py

RAW_EXTRACT_SCRIPTS = \
	$(RAW_NAME_BASIC) \
	$(RAW_TITLE_AKAS) \
	$(RAW_TITLE_BASICS) \
	$(RAW_TITLE_CREW) \
	$(RAW_TITLE_EPISODE) \
	$(RAW_TITLE_PRINCIPALS) \
	$(RAW_TITLE_RATINGS)

# ------------------------
# Targets
# ------------------------

.PHONY: up test clean

# Start MinIO container in detached mode
up:
	docker compose up -d

# Setup local Python env and run the script
extract:
	@echo "Setting up Python environment..."
	# Install Python and create virtualenv if needed
	pyenv install -s $(PYTHON_VERSION)
	pyenv virtualenv -f $(PYTHON_VERSION) $(VENV_NAME) || true
	# Activate venv, install deps, export env and run scripts in one shell
	. "$$(pyenv root)/versions/$(VENV_NAME)/bin/activate" && \
	pip install --upgrade pip && \
	pip install -r "$(REQ_FILE)" && \
	export S3_ENDPOINT="$(S3_ENDPOINT)" ACCESS_KEY="$(ACCESS_KEY)" SECRET_KEY="$(SECRET_KEY)" REGION="$(REGION)" && \
	for script in $(RAW_EXTRACT_SCRIPTS); do \
		python "$$script"; \
	done

# Stop MinIO and remove containers/volumes
clean:
	docker compose down -v
	pyenv uninstall -f $(VENV_NAME) || true
	rm -f .python-version
