#!/usr/bin/bash
for region in $(echo $REGIONS | sed "s/,/ /g")
do
  # deploy the app, note the BQ_DATASET and BQ_PROEJCTID values. Thats the dataset and project the updates will get written to
  gcloud run deploy iam-audit  --image gcr.io/$PROJECT_ID/iam_audit  \
    --region $region  --platform=managed --max-instances=1 --timeout=300s \
    --service-account=iam-audit-account@$PROJECT_ID.iam.gserviceaccount.com  --set-env-vars "REGION=$region" \
    --set-env-vars "BQ_DATASET=iam"  --set-env-vars "BQ_PROJECTID=$PROJECT_ID"  --set-env-vars "ORGANIZATION_ID=$ORGANIZATION_ID" -q
done