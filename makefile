PYTHON ?= python
UNITTEST ?= unittest
CTAGS ?= ctags

TESTDIR=tests

all: install test

install: clean
	$(PYTHON) setup.py install

# Reinstall with pip
reinstall: clean
	pip uninstall platform
	$(PYTHON) setup.py install

clean-ctags:
	rm -f tags

clean: clean-ctags
	$(PYTHON) setup.py clean --all
	rm -rf dist

test:
	$(PYTHON) -m $(UNITTEST) discover -s $(TESTDIR) -v
