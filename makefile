PYTHON ?= python
UNITTEST ?= unittest
CTAGS ?= ctags

all: install test

install: clean
	$(PYTHON) setup.py install

clean:
	echo "CLEAN TAGS"
	rm -f tags
	find . -type f -name '*.pyc' -delete
	$(PYTHON) setup.py clean --all
	rm -rf dist

test:
	$(PYTHON) -m $(UNITTEST) discover -v
