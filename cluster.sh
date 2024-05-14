gcloud dataproc clusters create cluster-b6d4 --enable-component-gateway --region us-central1 --master-machine-type n2-standard-4 --master-boot-disk-type pd-balanced --master-boot-disk-size 500 --num-workers 5 --worker-machine-type n2-standard-4 --worker-boot-disk-type pd-balanced --worker-boot-disk-size 500 --image-version 2.1-debian11 --optional-components JUPYTER --project cf-data-analytics --properties 'dataproc:dataproc.lineage.enabled=true'



export PHS_CLUSTER_NAME=phs-cluster
export REGION=us-central1
export ZONE=us-central1-b
export GCS_BUCKET=cf-spark-phs
export PROJECT_NAME=cf-data-analytics
gcloud dataproc clusters create $PHS_CLUSTER_NAME \
--enable-component-gateway \
--region ${REGION} --zone $ZONE \
--single-node \
--master-machine-type n2-highmem-4 \
--master-boot-disk-size 500 \
--image-version 2.2-debian12 \
--properties \
yarn:yarn.nodemanager.remote-app-log-dir=gs://$GCS_BUCKET/yarn-logs,\
mapred:mapreduce.jobhistory.done-dir=gs://$GCS_BUCKET/events/mapreduce-job-history/done,\
mapred:mapreduce.jobhistory.intermediate-done-dir=gs://$GCS_BUCKET/events/mapreduce-job-history/intermediate-done,\
spark:spark.eventLog.dir=gs://$GCS_BUCKET/events/spark-job-history,\
spark:spark.history.fs.logDirectory=gs://$GCS_BUCKET/events/spark-job-history,\
spark:SPARK_DAEMON_MEMORY=16000m,\
spark:spark.history.custom.executor.log.url.applyIncompleteApplication=false,\
spark:spark.history.custom.executor.log.url={{YARN_LOG_SERVER_URL}}/{{NM_HOST}}:{{NM_PORT}}/{{CONTAINER_ID}}/{{CONTAINER_ID}}/{{USER}}/{{FILE_NAME}} \
--project $PROJECT_NAME


