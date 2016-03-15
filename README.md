## Prerequisites ##

* Project A, whose datastore you want to monitor
* Project B, in which you want to run this code
* A system with the Google Cloud SDK configured for python deevelopment

## To Deploy this code ##

    git clone https://github.com/davidn/dsbackupmon.git
    cd dsbackupmon
    mkdir lib
    pip install -t lib -r requirements.txt
    gcloud preview app deploy app.yaml

## To configure logs ingestion ##

1. Configure Cloud Logging export of App Engine request_log to a Pub/Sub topic
in Project A
2. Go to https://dsbackupmon-dot-PROJECT-B.appspot.com/admin/create_cloud_metrics
3. In project B create a push subscription to the topic in project A, and have
   it push to https://dsbackupmon-dot-PROJECT-B.appspot.com/admin/insert

To send logs to cloud monitoring:
1. Use cron or some other system to run
   https://dsbackupmon-dot-PROJECT-B.appspot.com/admin/cloudmetric at the desired
   sampling frequency.
2. Use Google Cloud Monitoring with the new custom metric,
   custom.cloudmonitoring.googleapis.com/mapreduce_duration
