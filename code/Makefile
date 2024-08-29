SHELL := bash

all: requirements.txt

# Variables
REQUIREMENTS := requirements.txt
CONDA_ENV := spark
PYTHON_VERSION := 3.12

# Add packages to the list below to install them in the conda environment
PACKAGES := pyspark pyarrow requests ipython jupyterlab

# Recipes
$(REQUIREMENTS):
	$(MAKE) setup

.PHONY: setup install-requirements activate clean recreate

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
	conda install -c conda-forge $(PACKAGES) -y && \
	conda env export --no-builds > $(REQUIREMENTS)
	@echo "Packages installed. Environment exported to $(REQUIREMENTS)"

# Create the conda environment from the requirements.txt file
install-requirements:
	. $$(conda info --base)/etc/profile.d/conda.sh && \
	conda env create -f $(REQUIREMENTS)

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