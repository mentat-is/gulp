# uses buildkit, must be run like: docker build --ssh default --rm -t gulp .

FROM python:3.12.3-bullseye
ENV PYTHONUNBUFFERED=1

# install rust
RUN apt-get -qq update
RUN apt-get install -y -q \
    build-essential \
    libsystemd-dev \
    vim \
    sed \
    git \
    curl
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /app

# the following is to use the host ssh key when pulling private repos from github
# RUN mkdir /root/.ssh && ssh-keyscan github.com >>/root/.ssh/known_hosts

# copy template and requirements over
COPY ./gulp_cfg_template.json /app
COPY ./requirements.txt /app
COPY ./MANIFEST.in /app
COPY ./pyproject.toml /app
COPY ./src /app
COPY README.md /app

# install freezed requirements
# RUN --mount=type=ssh --mount=type=cache,target=/root/.cache pip3 install -r ./requirements.txt --no-dependencies
# and gulp itself, but remove our own dependencies (muty) already installed with the freezed requirements
#RUN sed -i '/muty-python/d' ./pyproject.toml
#RUN --mount=type=ssh --mount=type=cache,target=/root/.cache --mount=source=.git,target=.git,type=bind pip3 install -e .
RUN pip3 install .

# expose port 8080
EXPOSE 8080
