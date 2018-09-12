#!/bin/bash



for file in ./data/2011_biorario/net1/*.az
do
    echo $file 
    python ztd2postgresql.py -i "$file" >> log.txt
    python ztd2postgresql.py -i "$file" -n FALSE>> log.txt
done


for file in ./data/2011_biorario/net2/*.az
do
    echo $file 
    python ztd2postgresql.py -i "$file" >> log.txt
    python ztd2postgresql.py -i "$file" -n FALSE>> log.txt
done


for file in ./data/2011_biorario/net3/*.az
do
    echo $file 
    python ztd2postgresql.py -i "$file" >> log.txt
    python ztd2postgresql.py -i "$file" -n FALSE>> log.txt
done


