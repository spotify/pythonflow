FROM python:3
LABEL maintainer="Till Hoffmann <till@spotify.com>"

# Install requirements
COPY dev-requirements.txt dev-requirements.txt
RUN pip install --no-cache-dir -r dev-requirements.txt \
    && jupyter nbextension enable --py --sys-prefix widgetsnbextension \
    && ipcluster nbextension enable

# Install the package
COPY . /workspace
WORKDIR /workspace
RUN pip install -e .
