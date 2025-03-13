
FROM python:3.12.3-bullseye

# Add build arguments
ARG _VERSION
ENV _VERSION=${_VERSION}
ARG _MUTY_VERSION
ENV _MUTY_VERSION=${_MUTY_VERSION}

ENV PORT=8080
ENV ADDRESS=0.0.0.0

# install dependencies
RUN apt-get -qq update
RUN apt-get install -y -q \
    libsystemd-dev \
    vim \
    sed \
    git \
    curl

WORKDIR /app

# copy template and requirements over.
COPY ./src /app/src
COPY ./docs /app/docs
COPY ./gulp_cfg_template.json /app
COPY ./pyproject.docker.toml /app/pyproject.toml
COPY ./MANIFEST.in /app
COPY ./LICENSE.GULP.md /app
COPY ./LICENSE.AGPL-3.0.md /app
COPY ./LICENSE.md /app
COPY ./CONTRIBUTING.md /app
COPY ./README.md /app
COPY ./muty-python/src /app/muty-python/src
COPY ./muty-python/pyproject.docker.toml /app/muty-python/pyproject.toml
COPY ./muty-python/README.md /app/muty-python
COPY ./muty-python/LICENSE.md /app/muty-python

# copy requirements file if exists
COPY ./requirements.txt /app/requirements.txt

# set version passed as build argument
RUN echo "[.] GULP version: ${_VERSION}" && sed -i "s/version = .*/version = \"$(date +'%Y%m%d')+${_VERSION}\"/" /app/pyproject.toml
RUN echo "[.] muty-python version: ${_MUTY_VERSION}" && sed -i "s/version = .*/version = \"$(date +'%Y%m%d')+${_MUTY_VERSION}\"/" /app/muty-python/pyproject.toml
RUN echo "[.] Installing gulp"
RUN pip3 install --no-cache-dir --timeout=1000 -e .
RUN echo "[.] Installing muty-ptyhon"
RUN pip3 install --no-cache-dir --timeout=1000 -e ./muty-python

# should not be necessary but let's keep it for now
RUN export PYTHONPATH="$PYTHONPATH:/app/src"

# show python info and installed package list
RUN echo "[.] Python version: " && python3 --version
RUN echo "[.] Python sys.path: " && python3 -c "import sys; print('\n'.join(sys.path))"    
RUN echo "[.] Installed packages:" && pip3 list -v

# show version during build
RUN python3 -m gulp --version

EXPOSE ${PORT}

CMD ["sh","-c","gulp ${ARGS:---log-level debug}"]