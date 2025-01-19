# .devcontainer/Dockerfile
FROM mcr.microsoft.com/vscode/devcontainers/python:3.12

# Install OS dependencies
RUN apt update && export DEBIAN_FRONTEND=noninteractive && \
    apt -y install --no-install-recommends \
        rustc \
        docker.io \
        docker-compose \
        libsystemd-dev \
        jq \
        libpqxx-dev \
        git-lfs

# Install pipx
RUN python3 -m pip install --user pipx && \
    python3 -m pipx ensurepath

# setup docker
RUN curl -fsSL https://get.docker.com | sh && \
    groupadd -f docker && \
    usermod -aG docker vscode && \
    mkdir -p /run/docker && \
    chmod -R g+rwx /run/docker && \
    touch /var/run/docker.sock && \
    chmod 666 /var/run/docker.sock && \
    chown root:docker /var/run/docker.sock

# Clean up
RUN apt-get clean && rm -rf /var/lib/apt/lists/*

