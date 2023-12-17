!/bin/bash

mkdir -p triData
cd triData

year=2000
while [ $year -le 2022 ]
do
    wget "https://data.epa.gov/efservice/downloads/tri/mv_tri_basic_download/${year}_US/csv" -O "tri_data_${year}.$
    (( year++ ))
done
