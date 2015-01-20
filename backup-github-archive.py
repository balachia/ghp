import httplib2

from os.path import expanduser
from apiclient.discovery import build
from oauth2client.client import SignedJwtAssertionCredentials

# REPLACE WITH YOUR Project ID
project_id = 'compact-medium-830'
# REPLACE WITH THE SERVICE ACCOUNT EMAIL FROM GOOGLE DEV CONSOLE
client_id = '963370993614-mcbesf1lt0l72u7uab6seb92vu788drd@developer.gserviceaccount.com'

f = file(expanduser('~/Data/github/GithubBackup-0d697b8a6576.p12'), 'rb')
key = f.read()
f.close()

credentials = SignedJwtAssertionCredentials(
    client_id,
    key,
    scope='https://www.googleapis.com/auth/bigquery')

http = httplib2.Http()
http = credentials.authorize(http)

service = build('bigquery', 'v2', http=http)

jobCollection = service.jobs()
jobData = {
  'projectId': project_id,
  'configuration': {
    'extract': {
      'sourceTable': {
         'projectId': 'githubarchive',
         'datasetId': 'github',
         'tableId': 'timeline'
       },
      'destinationUris': ['gs://github-backup/backup-*.json.gz'],
      'destinationFormat': 'NEWLINE_DELIMITED_JSON',
      'compression': 'GZIP',
     }
   }
 }

insertJob = jobCollection.insert(projectId=project_id, body=jobData).execute()
import time
running=True
while running:
    status = jobCollection.get(projectId=project_id,
                               jobId=insertJob['jobReference']['jobId']).execute()
    print status
    if 'DONE' == status['status']['state']:
        print "Done exporting!"
        running=False
    else:
        print 'Waiting for export to complete..'
        time.sleep(10)

