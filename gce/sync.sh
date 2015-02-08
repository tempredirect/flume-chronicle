#!/bin/bash -e
readonly PROGNAME=$(basename $0)
readonly PROGDIR=$(dirname $0)
readonly ARGS="$@"

readonly BOOTSTRAP_LOCATION="gs://flume-chronicle-files"

upload() {
  gsutil -m -q rsync -r $PROGDIR $BOOTSTRAP_LOCATION
}

main() {
  echo -n "up..."
  upload
  echo "done"
}

main $ARGS
