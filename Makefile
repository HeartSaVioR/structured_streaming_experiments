help:
	@echo "clean - remove all build, test, coverage and Python artifacts"
	@echo "build - package"

all: default

# default: clean dev_deps deps test lint build
default: clean deps build

.venv:
	if [ ! -e ".venv/bin/activate_this.py" ] ; then virtualenv --clear .venv ; fi

# clean: clean-build clean-pyc clean-test
clean: clean-build clean-pyc

clean-build:
	rm -fr dist/

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

deps: .venv
	. .venv/bin/activate && pip install -U -r requirements.txt -t ./src/libs

build: clean
	mkdir ./dist
	cp ./src/main.py ./dist
	cd ./src && zip -x main.py -x \*libs\* -r ../dist/jobs.zip .
	cd ./src/libs && zip -r ../../dist/libs.zip .
