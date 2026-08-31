ARG BASE_IMAGE=python:3.11-slim-bookworm
FROM $BASE_IMAGE AS runtime-environment

# update pip and install uv
RUN python -m pip install -U "pip>=21.2"
RUN pip install uv

# install project requirements
COPY pyproject.toml uv.lock ./
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        git \
        openjdk-17-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# add kedro user
ARG KEDRO_UID=999
ARG KEDRO_GID=0
RUN groupadd -f -g ${KEDRO_GID} kedro_group && \
    useradd -m -d /home/kedro_docker -s /bin/bash -g ${KEDRO_GID} -u ${KEDRO_UID} kedro_docker

WORKDIR /home/kedro_docker
USER kedro_docker

FROM runtime-environment

# copy the whole project except what is in .dockerignore
ARG KEDRO_UID=999
ARG KEDRO_GID=0
COPY --chown=${KEDRO_UID}:${KEDRO_GID} . .
RUN uv sync --locked --no-dev
EXPOSE 8888

# CMD ["uv", "run", "kedro", "run"]
# CMD ["kedro", "run"]
