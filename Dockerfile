# configurable python version
ARG _PYTHON_VERSION=3.13
FROM python:${_PYTHON_VERSION}-bullseye

# Add build arguments
ARG _VERSION
ENV _VERSION=${_VERSION}
ARG _MUTY_VERSION
ENV _MUTY_VERSION=${_MUTY_VERSION}

# creates a non-root user with the same UID/GID as the build context user if possible
ARG _USER_NAME=gulp
ARG _USER_UID=1000
ARG _USER_GID=1000
RUN groupadd --gid $_USER_GID $_USER_NAME && \
    useradd --uid $_USER_UID --gid $_USER_GID -m $_USER_NAME

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
    curl

WORKDIR /app


# copy only requirements and pyproject.toml first for better caching
COPY ./requirements.txt /app/requirements.txt
COPY ./pyproject.toml /app/pyproject.toml

# install python dependencies
RUN pip3 install --timeout=1000 -r /app/requirements.txt || true

# now copy the rest of the source and assets
COPY ./src /app/src
COPY ./docs /app/docs
COPY ./gulp_cfg_template.json /app
COPY ./MANIFEST.in /app
COPY ./LICENSE.GULP.md /app
COPY ./LICENSE.AGPL-3.0.md /app
COPY ./LICENSE.md /app
COPY ./CONTRIBUTING.md /app
COPY ./README.md /app
COPY ./muty-python/src /app/muty-python/src
COPY ./muty-python/pyproject.toml /app/muty-python/pyproject.toml
COPY ./muty-python/README.md /app/muty-python
COPY ./muty-python/LICENSE.md /app/muty-python

# set version passed as build argument
RUN echo "[.] GULP version: ${_VERSION}" && sed -i "s/version = .*/version = \"$(date +'%Y%m%d')+${_VERSION}\"/" /app/pyproject.toml
RUN echo "[.] muty-python version: ${_MUTY_VERSION}" && sed -i "s/version = .*/version = \"$(date +'%Y%m%d')+${_MUTY_VERSION}\"/" /app/muty-python/pyproject.toml
RUN echo "[.] installing gulp ..."
RUN pip3 install --timeout=1000 -e .
RUN echo "[.] installing muty-ptyhon ..."
RUN pip3 install --timeout=1000 -e ./muty-python

# set permissions for the non-root user
RUN chown -R $_USER_NAME:$_USER_NAME /app

# switch to the non-root user
USER $_USER_NAME

# should not be necessary but let's keep it for now
# ENV PYTHONPATH="/app/src:${PYTHONPATH}"

# show python info and installed package list
RUN echo "[.] python version: " && python3 --version
RUN echo "[.] python sys.path: " && python3 -c "import sys; print('\n'.join(sys.path))"
RUN echo "[.] installed packages:" && pip3 list -v

# show version during build
RUN python3 -m gulp --version

EXPOSE ${GULP_BIND_TO_PORT}

CMD ["gulp", "--log-level", "warning"]