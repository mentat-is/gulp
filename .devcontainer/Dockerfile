# .devcontainer/Dockerfile
FROM mcr.microsoft.com/vscode/devcontainers/python:3.13-bullseye

# Install OS dependencies
RUN apt update && export DEBIAN_FRONTEND=noninteractive && \
    apt -y install --no-install-recommends \
    rustc \
    libsystemd-dev \
    jq \
    inetutils-ping \
    libpqxx-dev \
    git-lfs

# Install pipx
RUN python3 -m pip install --user pipx && \
    python3 -m pipx ensurepath

# setup docker
RUN curl -fsSL https://get.docker.com | sh && \
    apt -y install --no-install-recommends docker-compose && \
    # Create docker group if it doesn't exist
    groupadd -f docker && \
    usermod -aG docker vscode && \
    mkdir -p /run/docker 

# Clean up
RUN apt-get clean && rm -rf /var/lib/apt/lists/*

