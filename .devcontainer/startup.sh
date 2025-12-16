#!/usr/bin/env bash

curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sudo sh

echo "Ready"

echo "export PS1='$> '" >> ~/.bashrc

uv venv --allow-existing
source .venv/bin/activate

uv pip install pip
uv pip install databricks-connect==15.1.*

