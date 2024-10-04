# example build command:
# DOCKER_BUILDKIT=1 docker build --build-arg _VERSION=$(git describe --tags --always) --rm -t gulp .

FROM python:3.12.3-bullseye

# Add build argument for pip cache
ARG _VERSION
ENV _VERSION=${_VERSION}

# install dependencies
RUN apt-get -qq update
RUN apt-get install -y -q \
    libsystemd-dev \
    vim \
    sed \
    git \
    curl
# RUN curl https://sh.rustup.rs -sSf | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"
ENV PYTHONPATH="/pip-cache:/app"
ENV PATH="/usr/local/bin:${PATH}"

WORKDIR /app

# copy template and requirements over.
COPY ./src /app/src
COPY ./docs /app/docs
COPY ./gulp_cfg_template.json /app
COPY ./pyproject.toml /app
COPY ./LICENSE.GULP.md /app
COPY ./LICENSE.AGPL-3.0.md /app
COPY ./LICENSE.md /app
COPY ./CONTRIBUTING.md /app
COPY ./README.md /app

# copy requirements file if exists
COPY ./requirements.txt* /app/requirements.txt

# set version passed as build argument
RUN echo "[.] GULP version: ${_VERSION}" && sed -i "s/version = .*/version = \"$(date +'%Y%m%d')+${_VERSION}\"/" /app/pyproject.toml

RUN if [ -s /app/requirements.txt ]; then \
        # we patch the pyproject.toml to remove the dependencies so we can install the
        # frozen requirements from the host
        echo "[.] Patching pyproject.toml to remove dependencies" && \
        sed -i '/dependencies = \[/,/^\]/d' /app/pyproject.toml &&\
        echo "[.] Installing frozen requirements" &&\
        pip3 install -r ./requirements.txt && \
        pip3 install --no-cache-dir . ; \       
    else \
        echo "[.] No requirements.txt found, default (download pip packages)" && \   
        pip3 install --no-cache-dir . ; \       
    fi

# show installed package list
RUN pip3 list

# test run
RUN set -e && python3 -m gulp --version

# expose port 8080
EXPOSE 8080
