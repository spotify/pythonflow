.PHONY : all tests docs lint_tests code_tests clean install

all : tests docs

tests : lint_tests code_tests

lint_tests :
	pylint pythonflow

code_tests :
	py.test --cov pythonflow --cov-fail-under=100 --cov-report=term-missing --cov-report=html --verbose --durations=5 -s

docs :
	sphinx-build -b doctest docs build
	sphinx-build -nWT docs build

install :
	pip install -r requirements.txt
	pip install -e .

clean :
	rm -rf build/
