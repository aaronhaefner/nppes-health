SHELL := bash

all: requirements.txt

# Variables
REQUIREMENTS := requirements.txt
CONDA_ENV := spark
PYTHON_VERSION := 3.12
PACKAGES := pyspark pyarrow requests ipython jupyterlab pandas pre-commit ruff black

# Recipes
$(REQUIREMENTS):
	$(MAKE) setup

.PHONY: setup install-requirements activate clean recreate run setup-pre-commit run-pre-commit

# Create the conda environment and install the required packages
setup:
	conda init bash
	conda create -n $(CONDA_ENV) python=$(PYTHON_VERSION) -y
	@echo "Conda environment '$(CONDA_ENV)' created. To activate, run: make activate"
	@echo "Then, to install packages, run: make install-packages"

# Install packages in the conda environment
install-packages:
	@echo "Activating conda environment '$(CONDA_ENV)' and installing packages..."
	. $$(conda info --base)/etc/profile.d/conda.sh && \
	conda activate $(CONDA_ENV) && \
	pip install $(PACKAGES) && \
	pip freeze > $(REQUIREMENTS)
	@echo "Packages installed. Requirements exported to $(REQUIREMENTS)"

# Create the conda environment from the requirements.txt file
install-requirements:
	. $$(conda info --base)/etc/profile.d/conda.sh && \
	conda activate $(CONDA_ENV) && \
	pip install -r $(REQUIREMENTS)

# Provide instructions for activating the conda environment
activate:
	@echo "To activate the conda environment, run the following command in your shell:"
	@echo "source $$(conda info --base)/etc/profile.d/conda.sh && conda activate $(CONDA_ENV)"

# Delete the existing conda environment
clean:
	@echo "Deleting conda environment '$(CONDA_ENV)'..."
	conda env remove -n $(CONDA_ENV) -y

# Recreate the conda environment with the current settings
recreate: clean
	@echo "Recreating conda environment '$(CONDA_ENV)' with Python $(PYTHON_VERSION)..."
	$(MAKE) setup
	@echo "To complete setup, run: make activate"
	@echo "Then run: make install-packages"

# Run main.py using the correct Python interpreter
run:
	@echo "Running main.py with the correct Python interpreter..."
	. $$(conda info --base)/etc/profile.d/conda.sh && \
	conda activate $(CONDA_ENV) && \
	python -m main

# Set up pre-commit hooks
setup-pre-commit:
	@echo "Setting up pre-commit hooks..."
	. $$(conda info --base)/etc/profile.d/conda.sh && \
	conda activate $(CONDA_ENV) && \
	pre-commit install

# Run pre-commit hooks on all files
run-pre-commit:
	@echo "Running pre-commit hooks on all files..."
	. $$(conda info --base)/etc/profile.d/conda.sh && \
	conda activate $(CONDA_ENV) && \
	pre-commit run --all-files