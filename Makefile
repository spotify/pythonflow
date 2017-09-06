.PHONY: docker_image tests nbserver bash

docker_image : test-requirements.txt
	docker build -t pythonflow .

tests : docker_image
	docker run --rm pythonflow pylint pythonflow
	docker run --rm pythonflow py.test --cov --cov-fail-under=100 --cov-report=term-missing

README.md : docker_image README.ipynb
	docker run --rm -v "$$PWD":/pythonflow pythonflow jupyter nbconvert README.ipynb --execute --to markdown

nbserver : docker_image
	docker run --rm -v "$$PWD":/pythonflow -p 9000:8888 pythonflow jupyter notebook --allow-root --ip=0.0.0.0 --no-browser

bash :
	docker run --rm -v "$$PWD":/pythonflow -it pythonflow bash
