PYTHON ?= python3
FLAKE ?= pyflakes
PEP ?= pep8

.PHONY: all flake doc test cov dist
all: flake doc cov

doc:
	make -C docs html

flake:
	$(FLAKE) aionsq tests examples
	$(PEP) aionsq tests examples

test:
	$(PYTHON) runtests.py -v 4

cov coverage:
	$(PYTHON) runtests.py --coverage

dist:
	-rm -r build dist aioredis.egg-info
	$(PYTHON) setup.py sdist bdist_wheel

clean:
	find . -name __pycache__ |xargs rm -rf
	find . -type f -name '*.py[co]' -delete
	find . -type f -name '*~' -delete
	find . -type f -name '.*~' -delete
	find . -type f -name '@*' -delete
	find . -type f -name '#*#' -delete
	find . -type f -name '*.orig' -delete
	find . -type f -name '*.rej' -delete
	rm -f .coverage
	rm -rf coverage
	rm -rf docs/_build
