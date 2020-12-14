from google.cloud import bigquery
from google.oauth2 import service_account
import datetime
import pytz

credentials = service_account.Credentials.from_service_account_file("credentials/bigquery.json", scopes=["https://www.googleapis.com/auth/cloud-platform"])
project = credentials.project_id
client = bigquery.Client(credentials=credentials, project=project)

old_days = 60
now = datetime.datetime.now().replace(tzinfo=pytz.UTC)

datasets = list(client.list_datasets())

for dataset in datasets:
    tables = list(client.list_tables(dataset.dataset_id))
    for table in tables:
        t = client.get_table(table.reference)
        if t.modified < now - datetime.timedelta(days=old_days):
            print('{}.{}'.format(dataset.dataset_id, table.table_id))