#!/usr/bin/env bash
airflow db upgrade
airflow users create -r Admin -u admin -e bruno@bruno.com -f admin -l admin -p admin
airflow webserver
