import datetime
from collections import defaultdict
from oauth2client.client import GoogleCredentials
from apiclient.discovery import build
import re
import logging
import webapp2
from google.appengine.ext import ndb
from google.appengine.ext.ndb import msgprop
from protorpc import messages
import base64
import json
import urllib
from google.appengine.api.app_identity import get_application_id


DEBUG=True
DURATION_METRIC="custom.cloudmonitoring.googleapis.com/mapreduce_duration"
MR_ID_LABEL="custom.cloudmonitoring.googleapis.com/mapreduce/id"

if DEBUG:
  logger=logging.getLogger()
  logger.setLevel(logging.DEBUG)
  import httplib2
  httplib2.debuglevel=4

def cloudmonitoring():
  credentials = GoogleCredentials.get_application_default()
  return build('cloudmonitoring', 'v2beta2', credentials=credentials)


class State(messages.Enum):
  UNKNOWN = 0
  SEEN_BEFORE_KICKOFF = 1
  KICKOFF = 2
  PROCESSING = 3
  FINALIZED = 4
  SEEN_AFTER_FINALIZE = 5


class MapReduceEvent(ndb.Model):
  mr_id = ndb.StringProperty()
  start_time = ndb.DateTimeProperty()
  end_time = ndb.DateTimeProperty()
  step = ndb.StringProperty()


class CreateCustomMetrics(webapp2.RequestHandler):
  def get(self):
    cm = cloudmonitoring()
    cm.metricDescriptors().create(
      project=get_application_id(),
      body={"name": DURATION_METRIC,
            "project": get_application_id(),
            "typeDescriptor": {"metricType":"gauge","valueType":"double"},
            "labels":[
                {"key":MR_ID_LABEL, "description":"Which MapReduce this is taking this time."}]}).execute()


class Blank(webapp2.RequestHandler):
  def get(self):
    pass


class SubmitMetrics(webapp2.RequestHandler):
  def get(self):
    now=datetime.datetime.now()

    # we assume that the finalize comes after the start, so we go through the
    # events in reverse order looking for events that started by never ended
    unfinished = dict()
    for event in MapReduceEvent.query().order(MapReduceEvent.start_time):
      if event.step == "kickoffjob_callback":
        unfinished[event.mr_id]=event
      elif event.step == "finalizejob_callback":
        unfinished.pop(event.mr_id,0)

    logging.info("logging events %s" % unfinished)

    timeseries = []
    for event in unfinished.itervalues():
      timeseries.append({
          "timeseriesDesc": {"project":get_application_id(),
                             "metric":DURATION_METRIC,
                             "labels":{MR_ID_LABEL:event.mr_id}},
          "point": {
            "start": now.replace(microsecond=0).isoformat()+"Z",
            "end": now.replace(microsecond=0).isoformat()+"Z",
            "doubleValue": (now-event.start_time).total_seconds()}})

    if len(timeseries) > 0:
      cm = cloudmonitoring()
      cm.timeseries().write(
        project=get_application_id(),
        body={"timeseries":timeseries}).execute()


class InsertPage(webapp2.RequestHandler):
  def post(self):
    message = json.loads(urllib.unquote(self.request.body).rstrip('='))
    log_entry = json.loads(base64.b64decode(str(message['message']['data'])))
    logging.debug(log_entry)

    match = re.match(r"/_ah/mapreduce/(\w*)/(\w*)", log_entry["protoPayload"]["resource"])
    if not match:
      logging.debug("Not mapreduce, ignoring")
      self.response.status = 204
      return

    step, mr_id = match.groups()
    if step in ["worker_callback", "controller_callback"]:
      logging.debug("not start/end, ignoring")
      self.response.status = 204
      return

    start_time = datetime.datetime.strptime(log_entry["protoPayload"]["startTime"], "%Y-%m-%dT%H:%M:%S.%fZ")
    end_time = datetime.datetime.strptime(log_entry["protoPayload"]["endTime"], "%Y-%m-%dT%H:%M:%S.%fZ")
    mr = MapReduceEvent(mr_id=mr_id, step=step, start_time=start_time, end_time=end_time)
    mr.put()


application = webapp2.WSGIApplication([
    webapp2.Route(r'/', Blank),
    webapp2.Route(r'/insert', InsertPage),
    webapp2.Route(r'/cloudmetrics', SubmitMetrics),
    webapp2.Route(r'/admin/create_custom_metrics', CreateCustomMetrics),
], debug=True)
