#!/bin/bash -e
readonly PROGNAME=$(basename $0)
readonly PROGDIR=$(dirname $0)
readonly ARGS="$@"

readonly PROJECT="flume-chronicle-1"
readonly ZONE="us-central1-a"
readonly BOOTSTRAP_LOCATION="gs://flume-chronicle-files"

new_node() {
    local name=$1
    gcloud compute --project ${PROJECT} instances create ${name} \
                   --zone ${ZONE} \
                   --machine-type "n1-standard-1" \
                   --network "default" \
                   --metadata "startup-script-url=$BOOTSTRAP_LOCATION/bootstrap.sh" \
                   --maintenance-policy "MIGRATE" \
                   --scopes "https://www.googleapis.com/auth/devstorage.read_only" \
                   --image "https://www.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/ubuntu-1404-trusty-v20150128" \
                   --boot-disk-type "pd-standard" \
                   --boot-disk-device-name ${name}
}

delete_node() {
    local name=$1

    gcloud compute --project ${PROJECT} \
                    instances delete \
                    --zone ${ZONE} \
                    -q \
                    ${name}
}

ssh_node() {
    local name=$1

    gcloud compute --project ${PROJECT} \
                    ssh \
                    --zone ${ZONE} \
                    ${name}
}

main() {
  local command=$1
  local node=$2
  echo -n "$command $node..."
  case $command in
    create ) new_node $node ;;
    delete ) delete_node $node ;;
    ssh ) ssh_node $node ;;
    * )
      echo "command required, up|down"
      exit 1
      ;;
  esac
  echo "done"
}

main $ARGS


