#!/usr/bin/bash
# incase you don't want to wait on tahiti time

i=0
for region in $(echo $REGIONS | sed "s/,/ /g")
do
  gcloud scheduler jobs run iam-scheduler-$region  
  ((i=i+1))
  sleep 120
done