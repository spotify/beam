#!/bin/bash

set -e -u -f -o pipefail

SRC=smb-wip

for DST in smb-metadata smb-fileops smb-sink smb-source smb-api smb-benchmark; do
    echo "============================================================"
    echo "Updating branch $DST"

    git checkout $DST
    echo "Updating files"
    for FILE in $(git diff master --name-only); do
        git checkout $SRC -- $FILE
    done

    if [ -z "$(git status --untracked-files=no --porcelain)" ]; then
        echo "Nothing to commit"
    else
        echo "Applying Spotless"
        ./gradlew :sdks:java:extensions:smb:spotlessApply

        echo "Building module"
        ./gradlew -p sdks/java/extensions/smb build

        echo "Committing changes"
        git commit -m "Update changes from WIP branch" || true

        git show HEAD
        git push
    fi
done

git checkout $SRC
