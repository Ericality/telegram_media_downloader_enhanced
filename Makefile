TEST_ARTIFACTS ?= /tmp/coverage

.PHONY: install dev_install test

install:
	python3 -m pip install --upgrade pip setuptools
	python3 -m pip install ./pyrogram-patch.zip
	python3 -m pip install -r requirements.txt

dev_install: install
	python3 -m pip install -r dev-requirements.txt

test:
	py.test --cov=core --cov=services --cov=workers --cov=media_downloader \
		--cov-report term-missing \
		--cov-report html:${TEST_ARTIFACTS} \
		--junit-xml=${TEST_ARTIFACTS}/media-downloader.xml \
		tests/
