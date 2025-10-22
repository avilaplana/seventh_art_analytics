# ------------------------
# Variables
# ------------------------
PYTHON_VERSION ?= 3.11.7       # Python version for pyenv
VENV_NAME ?= extract-env
REQ_FILE = requirements.txt
SCRIPT = src/extract_to_s3.py

S3_ENDPOINT = http://localhost:9000
ACCESS_KEY = minioadmin
SECRET_KEY = minioadmin123
REGION = eu-west-2
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
	# Activate the virtualenv explicitly and run everything in one shell
	. $$(pyenv root)/versions/$(VENV_NAME)/bin/activate && \
	pip install --upgrade pip && \
	pip install -r $(REQ_FILE) && \
	S3_ENDPOINT=$(S3_ENDPOINT) \
	ACCESS_KEY=$(ACCESS_KEY) \
	SECRET_KEY=$(SECRET_KEY) \
	REGION=$(REGION) \
	python $(SCRIPT)

# Stop MinIO and remove containers/volumes
clean:
	docker compose down -v
	pyenv uninstall -f $(VENV_NAME) || true
	rm -f .python-version
