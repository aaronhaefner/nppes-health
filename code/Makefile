SHELL := bash

all: requirements.txt

# Variables
REQUIREMENTS := requirements.txt
CONDA_ENV := spark
PYTHON_VERSION := 3.10
PACKAGES := pyspark pyarrow requests ipython jupyterlab

# Recipes
$(REQUIREMENTS):
	$(MAKE) setup

.PHONY: setup install-requirements

# Create the conda environment and install the required packages
setup:
	conda init bash
	conda create -n $(CONDA_ENV) python=$(PYTHON_VERSION) -y
	# Activate the conda environment in the same shell session
	. $$(conda info --base)/etc/profile.d/conda.sh && \
	conda activate $(CONDA_ENV) && \
	conda install -c conda-forge $(PACKAGES) -y && \
	conda env export --no-builds > $(REQUIREMENTS)

# Create the conda environment from the requirements.txt file
install-requirements:
	. $$(conda info --base)/etc/profile.d/conda.sh && \
	conda env create -f $(REQUIREMENTS)
