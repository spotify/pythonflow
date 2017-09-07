.PHONY: docker_image docker_nbserver tests lint_tests code_tests docs

docker_image : requirements/*.txt
	docker build -t pythonflow .

docker_nbserver : docker_image
	docker run --rm -v "$$PWD":/pythonflow -p 9000:8888 pythonflow jupyter notebook --allow-root --ip=0.0.0.0 --no-browser

docker_bash : docker_image
	docker run --rm -v "$$PWD":/pythonflow -it pythonflow bash

tests : lint_tests code_tests

lint_tests :
	pylint pythonflow

code_tests :
	py.test --cov pythonflow --cov-fail-under=100 --cov-report=term-missing

docs :
	sphinx-build -b doctest docs build
	sphinx-build -nWT docs build
