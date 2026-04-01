# configurable python version
ARG _PYTHON_VERSION=3.13
FROM python:${_PYTHON_VERSION}-bullseye

# Add build arguments
ARG _VERSION
ENV _VERSION=${_VERSION}

# creates a non-root user with the same UID/GID as the build context user if possible
ARG _USER_NAME=gulp
ARG _USER_UID=1000
ARG _USER_GID=1000
RUN groupadd --gid $_USER_GID $_USER_NAME && \
    useradd --uid $_USER_UID --gid $_USER_GID -m $_USER_NAME
ENV GULP_RUNTIME_USER=${_USER_NAME}

ENV GULP_BIND_TO_PORT=8080
ENV GULP_BIND_TO_ADDR=0.0.0.0

# install dependencies
RUN apt-get -qq update
RUN apt-get install -y -q \
    libsystemd-dev \
    inetutils-ping \
    vim \
    sed \
    git \
    curl \
    rsyslog \
    gosu

WORKDIR /gulp


# copy only pyproject.toml first for better caching
COPY ./pyproject.toml /gulp/pyproject.toml

# now copy the rest of the source and assets
COPY ./src /gulp/src
COPY ./docs /gulp/docs
COPY ./MANIFEST.in /gulp
COPY ./LICENSE.GULP.md /gulp
COPY ./LICENSE.AGPL-3.0.md /gulp
COPY ./LICENSE.md /gulp
COPY ./CONTRIBUTING.md /gulp
COPY ./README.md /gulp
COPY ./docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
COPY ./tests /gulp/tests
COPY ./samples /gulp/samples

# set version passed as build argument and use it for setuptools_scm
RUN echo "[.] GULP version: ${_VERSION}" && \
    export GULP_BUILD_VERSION="$(date +'%Y%m%d')+${_VERSION}" && \
    echo "[.] using setuptools_scm pretend version: ${GULP_BUILD_VERSION}" && \
    SETUPTOOLS_SCM_PRETEND_VERSION="${GULP_BUILD_VERSION}" \
    SETUPTOOLS_SCM_PRETEND_VERSION_FOR_MENTAT_GULP="${GULP_BUILD_VERSION}" \
    pip3 install --timeout=1000 -e .

# set permissions for the non-root user
RUN chown -R $_USER_NAME:$_USER_NAME /gulp
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# show python info and installed package list
RUN echo "[.] python version: " && python3 --version
RUN echo "[.] python sys.path: " && python3 -c "import sys; print('\n'.join(sys.path))"
RUN echo "[.] installed packages:" && pip3 list -v

# show version during build
RUN python3 -m gulp --version

EXPOSE ${GULP_BIND_TO_PORT}

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["gulp", "--log-level", "warning"]