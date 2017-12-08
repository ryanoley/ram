PYTHON ?= python
UNITTEST ?= unittest

all: install test

install: clean
	$(PYTHON) ram/config.py
	$(PYTHON) setup.py install

clean:
	echo "CLEANING..."
	# Delete .pyc files
	find . -type f -name '*.pyc' -delete
	# Delete empty directories
	find . -type d -empty -delete
	# Distutils version of clean
	$(PYTHON) setup.py clean --all
	rm -rf dist

test:
	$(PYTHON) -m $(UNITTEST) discover -v
