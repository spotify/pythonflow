.PHONY : all tests docs lint_tests code_tests clean

all : tests docs

tests : lint_tests code_tests

lint_tests :
	pylint pythonflow

code_tests :
	py.test --cov pythonflow --cov-fail-under=100 --cov-report=term-missing --cov-report=html -v

docs :
	sphinx-build -b doctest docs build
	sphinx-build -nWT docs build

clean :
	rm -rf build/
