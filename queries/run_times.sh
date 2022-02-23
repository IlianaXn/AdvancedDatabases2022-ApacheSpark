#!/bin/bash

for i in {1..5}
do
	spark-submit RDD_query$i.py
	spark-submit SQL_query$i.py F
	spark-submit SQL_query$i.py T
done
