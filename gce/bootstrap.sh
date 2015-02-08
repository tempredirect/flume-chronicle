#!/bin/bash -e

readonly PROGNAME=$(basename $0)
readonly PROGDIR=$(dirname $0)
readonly ARGS="$@"

readonly BOOTSTRAP_LOCATION="gs://flume-chronicle-files"

set -x

apt_add_zulu_repo() {
    apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 0x219BD9C9
    apt-add-repository 'deb http://repos.azulsystems.com/ubuntu stable main'
}

apt_update() {
  apt-get update && apt-get -y upgrade
}

apt_install() {
  local packages=$@
  apt-get -y install $packages
}

install_hadoop_cli() {
    gsutil cp $BOOTSTRAP_LOCATION/hadoop-2.6.0.tar.gz /tmp/hadoop-2.6.0.tar.gz
    rm -rf /opt/hadoop-2.6.0
    cd /opt
    tar xfvp /tmp/hadoop-2.6.0.tar.gz

    gsutil cp $BOOTSTRAP_LOCATION/hadoop-env.sh /opt/hadoop-2.6.0/etc/hadoop/hadoop-env.sh
}

install_flume() {
    gsutil cp $BOOTSTRAP_LOCATION/apache-flume-1.5.2-bin.tar.gz /tmp/apache-flume-1.5.2-bin.tar.gz
    rm -rf /opt/apache-flume-1.5.2-bin
    cd /opt
    tar xfvp /tmp/apache-flume-1.5.2-bin.tar.gz

    mkdir -p /flume
    mkdir -p /etc/flume

    gsutil cp $BOOTSTRAP_LOCATION/flume-conf.properties /etc/flume/flume-conf.properties
    gsutil cp $BOOTSTRAP_LOCATION/flume-env.sh /etc/flume/flume-env.sh
    gsutil cp $BOOTSTRAP_LOCATION/flume.conf /etc/init/flume.conf
}

install_benchmark() {
    mkdir -p /etc/flume
    gsutil cp $BOOTSTRAP_LOCATION/postbody.txt /etc/flume
    gsutil cp $BOOTSTRAP_LOCATION/flume_benchmark1.conf /etc/init
}

main() {
    apt_add_zulu_repo

    apt_update

    apt_install zulu-8 sysstat apache2-utils

    install_hadoop_cli
    install_flume
    install_benchmark
}

main
