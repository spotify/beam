#!/bin/bash

set -e -u -f -o pipefail

if [ $# -ne 2 ]; then
    echo "Usage: $(basename $0) SRC_BRANCH DST_BRANCH"
    exit 0
fi

SRC=$1
DST=$2

#echo "Updating master"
#git fetch origin
#git fetch upstream
#git checkout master
#git reset --hard upstream/master
#git push
#
#echo "Updating source branch $SRC"
#git checkout $SRC
#git rebase upstream/master
#git push --force
#
#echo "Updating destination branch $DST"
#git checkout $DST
#git reset --hard origin/$DST
#git rebase upstream/master

git checkout $DST
echo "Updating files"
for FILE in $(git diff master --name-only); do
    git checkout $SRC -- $FILE
done

echo "Applying Spotless"
./gradlew :sdks:java:extensions:smb:spotlessApply

echo "Building module"
./gradlew -p sdks/java/extensions/smb build

echo "Committing changes"
git commit -m "Update changes from WIP branch"

git show HEAD
