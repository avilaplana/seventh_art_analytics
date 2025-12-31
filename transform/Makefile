# ------------------------
# Variables
# ------------------------
PYTHON_VERSION ?= 3.11.7       # Python version for pyenv
VENV_NAME ?= transform-env
REQ_FILE = requirements.txt

.PHONY: transform

# Setup local Python env and run the script
transform:
	@echo "Setting up Python environment..."
	# Install Python and create virtualenv if needed
	pyenv install -s $(PYTHON_VERSION)
	pyenv virtualenv -f $(PYTHON_VERSION) $(VENV_NAME) || true
	# Activate venv, install deps, export env and run scripts in one shell
	. "$$(pyenv root)/versions/$(VENV_NAME)/bin/activate" && \
	pip install --upgrade pip && \
	pip install -r "$(REQ_FILE)"	
	

# Stop MinIO and remove containers/volumes
clean:
	pyenv uninstall -f $(VENV_NAME) || true
	rm -f .python-version
