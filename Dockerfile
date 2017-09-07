FROM python:3
MAINTAINER Till Hoffmann <till@spotify.com>

# Install requirements
COPY requirements requirements
RUN pip install -r requirements/docs.txt -r requirements/test.txt -r requirements/deployment.txt

# Install the package
COPY . pythonflow
WORKDIR pythonflow
RUN pip install -e .
