# ------------------------
# Variables
# ------------------------
PYTHON_VERSION ?= 3.11.7       # Python version for pyenv
VENV_NAME ?= bronze-env
REQ_FILE = requirements.txt
SCRIPT = upload_to_s3.py

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
test:
	# Install Python via pyenv if not already installed
	pyenv install -s $(PYTHON_VERSION)
	# Create virtualenv if not exists
	pyenv virtualenv -f $(PYTHON_VERSION) $(VENV_NAME) || true
	# Activate virtualenv locally
	pyenv local $(VENV_NAME)
	# Upgrade pip and install dependencies
	pip install --upgrade pip
	pip install -r $(REQ_FILE)
    # Inject environment variables and run the script
	S3_ENDPOINT=$(S3_ENDPOINT) \
	ACCESS_KEY=$(ACCESS_KEY) \
	SECRET_KEY=$(SECRET_KEY) \
	REGION=$(REGION) \
	python $(SCRIPT)

# Stop MinIO and remove containers/volumes
clean:
	docker compose down -v
