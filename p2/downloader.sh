#!/bin/sh

for i in $(seq 1949 2022):
do 
    wget https://www.ncei.noaa.gov/pub/data/noaa/$i/065900-99999-$i.gz
done
