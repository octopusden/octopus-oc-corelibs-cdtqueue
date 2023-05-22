ARG PYTHON_VERSION=3.7
FROM python:${PYTHON_VERSION}

RUN rm -rf /build
COPY --chown=root:root . /build
WORKDIR /build
USER root
RUN python -m pip install $(pwd) && python -m unittest discover -v && python setup.py bdist_wheel
