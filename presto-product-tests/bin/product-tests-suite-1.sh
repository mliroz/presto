#!/usr/bin/env bash

set -xeuo pipefail

presto-product-tests/bin/run_on_docker.sh \
    multinode \
    -x quarantine,big_query,storage_formats,profile_specific_tests,tpcds,tpch,cassandra,mysql_connector,postgresql_connector,mysql,kafka,avro,big_query
