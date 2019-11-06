#!/bin/sh

set -e

CLONE_LOGS="$(dirname $(dirname $0))/clone_logs.py"

if [ "$#" != 1 ]; then
    echo "usage: $0 N"
    exit 1
fi

CURR="$1"
SUCC="$(($1 + 1))"

DATABASE="${CURR}.sqlite3"
ARCHIVE="${CURR}00k-to-${SUCC}00k.7z"

echo "processing: $DATABASE, compressing to $ARCHIVE"

echo "downloading"

$CLONE_LOGS \
    -d $DATABASE \
    -r "${CURR}00000" "${SUCC}00000"

echo "vacuuming"

sqlite3 $DATABASE "vacuum"

echo "compressing"

7z a -t7z -m0=lzma -mx=9 -mfb=64 -md=32m -ms=on $ARCHIVE $DATABASE

echo "done"
