#!/bin/sh
mvn install:install-file -Dfile=./lib/hbase-0.90.0.jar -DgroupId=org.apache.hbase -DartifactId=hbase -Dversion=0.90.0 -Dpackaging=jar
mvn install:install-file -Dfile=./lib/terrastore-javaclient-2.1.jar -DgroupId=terrastore-javaclient -DartifactId=terrastore-javaclient -Dversion=2.1 -Dpackaging=jar
