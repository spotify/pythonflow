FROM python:3
LABEL maintainer="Till Hoffmann <till@spotify.com>"

# Install requirements
COPY dev-requirements.txt dev-requirements.txt
RUN pip install --no-cache-dir -r dev-requirements.txt

# Install the package
COPY . /workdir
WORKDIR /workdir
RUN pip install -e .
