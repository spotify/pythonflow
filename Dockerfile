FROM python:3
MAINTAINER Till Hoffmann <till@spotify.com>

# Install requirements
COPY dev-requirements.txt dev-requirements.txt
RUN pip install -r dev-requirements.txt

# Install the package
COPY . pythonflow
WORKDIR pythonflow
RUN pip install -e .
