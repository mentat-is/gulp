#!/usr/bin/env bash
# checkout to the given branch the following repos (the same branch must be present on ALL repos!)
if [ -z "$1" ]; then
  echo "Usage: $0 <branch-name>"
  exit 1
fi

# list of repos to checkout
_branch=$1
_repos=("gulp-paid-plugins" "gulp-sdk-python")
_pwd=$(pwd)
for repo in "${_repos[@]}"; do
    echo "checking out branch '$_branch' in repo '$repo'"
    (cd "./$repo" && git checkout $1 && git pull)
    if [ $? -ne 0 ]; then
        echo "Error checking out branch '$_branch' in repo '$repo'"
        exit 1
    fi
    cd "$_pwd"
done
git checkout "$_branch"
git pull


