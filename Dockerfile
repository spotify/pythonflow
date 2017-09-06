FROM python:3
MAINTAINER Till Hoffmann <till@spotify.com>

# Install requirements
COPY test-requirements.txt test-requirements.txt
RUN pip install -r test-requirements.txt

# Install the package
COPY . pythonflow
WORKDIR pythonflow
RUN pip install -e .
