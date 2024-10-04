#!/usr/bin/env bash
# this should be run when pushing to master branch or stable tag to freeze requirements to requirements.txt
echo [.] Freezing requirements to requirements.txt
pip3 freeze >/tmp/requirements.txt

echo [.] Removing gulp from freezed requirements.txt
sed '/gulp@/d' /tmp/requirements.txt >/tmp/requirements2.txt
sed 's/^-e //g' /tmp/requirements2.txt >./requirements.txt
cat ./requirements.txt

echo [.] Done, now add requirements.txt, commit and force push to master branch or stable tag.
