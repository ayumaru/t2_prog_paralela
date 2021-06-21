#!/bin/bash


for i in {0..20}
do
    echo "************"
    time ./dna 4 |grep "real"
    echo "***********"
done