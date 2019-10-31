.PHONY: help install-deps compile-deps prepare-deps git-hooks pow

help:
	@echo "Available commands: help install-deps compile-deps"

install-deps: prepare-deps
	pip-sync requirements.txt requirements-dev.txt

compile-deps: prepare-deps
	pip-compile --no-index --no-emit-trusted-host requirements.in
	pip-compile --no-index --no-emit-trusted-host requirements-dev.in

prepare-deps:
	@[ -n "$${VIRTUAL_ENV}" ] || (echo >&2 "Please activate virtualenv."; false)
	pip install -U pip==19.2.3 setuptools==41.4.0 wheel==0.33.6 pip-tools==4.2.0

git-hooks:
	ln -sf `pwd`/tools/git-hooks/* .git/hooks/

pow:
	echo "http://$$(docker-compose port wsgi 5000)" > ~/.pow/huskar.test
