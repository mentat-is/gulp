#!/usr/bin/env bash
# freeze requirements to requirements.txt
echo [.] Freezing requirements to requirements.txt
pip3 freeze >/tmp/requirements.txt

echo [.] Removing gulp from freezed requirements.txt
sed '/gulp@/d' /tmp/requirements.txt >/tmp/requirements2.txt
sed 's/^-e //g' /tmp/requirements2.txt >./requirements.txt
cat ./requirements.txt