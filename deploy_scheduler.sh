#!/usr/bin/bash

i=0
for region in $(echo $REGIONS | sed "s/,/ /g")
do
  # set permissions to allow cloud scheduler to call cloud run
  gcloud run  services add-iam-policy-binding iam-audit --region=$region --member=serviceAccount:schedulerunner@$PROJECT_ID.iam.gserviceaccount.com --role=roles/run.invoker

  export RUN_URL=`gcloud run services describe iam-audit --region=$region --format="value(status.address.url)"`

  # run every morning at 1am bora-bora time; stagger two minutes to allow for iam API rate limit quota.
  # TODO: account for more than 30 regions since that'll exceed the cron parameter (i.,e mod in to next hour)
  gcloud scheduler jobs create http iam-scheduler-$region --http-method=GET --schedule "$i 1 * * *" \
    --attempt-deadline=420s --time-zone="Pacific/Tahiti" \
    --oidc-service-account-email=schedulerunner@$PROJECT_ID.iam.gserviceaccount.com  \
    --oidc-token-audience=$RUN_URL --uri=$RUN_URL
  ((i=i+2))
done