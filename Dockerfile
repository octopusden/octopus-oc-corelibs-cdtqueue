ARG PYTHON_VERSION=3.7
ARG DOCKER_REGISTRY_HOST=${DOCKER_REGISTRY_HOST}
FROM ${DOCKER_REGISTRY_HOST}/base/python-${PYTHON_VERSION}:master

COPY . /local/cdtqueue

WORKDIR /local/cdtqueue

RUN python setup.py sdist bdist_wheel && python -m pip install dist/*.whl && python setup.py test
