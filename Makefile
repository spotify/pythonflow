.PHONY: image tests lint_tests code_tests docs sdist testpypi pypi

image : dev-requirements.txt
	docker build -t pythonflow .

tests : lint_tests code_tests

lint_tests :
	pylint pythonflow

code_tests :
	py.test --cov pythonflow --cov-fail-under=100 --cov-report=term-missing --cov-report=html

docs :
	sphinx-build -b doctest docs build
	sphinx-build -nWT docs build

sdist :
	python setup.py sdist

testpypi : sdist
	twine upload --repository-url https://test.pypi.org/legacy/ dist/pythonflow-*

pypi : sdist
	twine upload dist/pythonflow-*
