import argparse
import sys
import os
import requests
import json
from requests.auth import HTTPBasicAuth 
from math import ceil
from sodapy import Socrata
parser = argparse.ArgumentParser(description='Process data')
parser.add_argument('--page_size', type=int, help='how many rows to get per page', required=True)
parser.add_argument('--num_pages', type=int, help='how many pages to get in total')

args = parser.parse_args(sys.argv[1:])
DATASET_ID = os.environ["DATASET_ID"]
APP_TOKEN = os.environ["APP_TOKEN"]
ES_HOST = os.environ["ES_HOST"]
INDEX_NAME = os.environ["INDEX_NAME"]
ES_USERNAME = os.environ["ES_USERNAME"]
ES_PASSWORD = os.environ["ES_PASSWORD"]

domain = "data.cityofnewyork.us"
client = Socrata(domain, APP_TOKEN)
data_size = client.get(DATASET_ID, select='COUNT(*)')[0]
num_rows = int(str(data_size)[11:19])
    
if args.num_pages == None:
    page_size = args.page_size
    num_pages = int(ceil(num_rows/page_size))
else:
    page_size = args.page_size
    num_pages = args.num_pages  
    
if __name__ == "__main__":
    try:
        r = requests.put(f"{ES_HOST}/{INDEX_NAME}",
            auth=HTTPBasicAuth(ES_USERNAME,ES_PASSWORD),
            json={
                "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 1
                }, 
                "mappings": {"properties": {
                    "plate": { "type": "keyword" },
                    "state": { "type": "keyword" },
                    "license_type": { "type": "keyword" },
                    "summons_number": { "type": "keyword" },
                    "issue_date": { "type": "date", "format": "mm/dd/yyyy" },
                    "violation_time": {"type": "keyword"},
                    "violation": { "type": "keyword" },
                    "fine_amount": { "type": "float" },
                    "penalty_amount": { "type": "float" },
                    "interest_amount": { "type": "float" },
                    "reduction_amount": { "type": "float" },
                    "payment_amount": { "type": "float" },
                    "amount_due": { "type": "float" },
                    "precinct": { "type": "keyword" },
                    "county": { "type": "keyword" },
                    "issuing_agency": { "type": "keyword" }
                    }
                }
            })
        
        print(r.status_code)
        r.raise_for_status()
    except Exception as e:
        print(e)
        
    es_rows = []
    for i in range(0,num_pages):
        rows = Socrata("data.cityofnewyork.us", APP_TOKEN).get(DATASET_ID, limit=page_size, offset=i*(page_size))
        for row in rows:
            try:
                es_row = {}
                es_row["plate"] = row["plate"]
                es_row["state"] = row["state"]
                es_row["license_type"] = row["license_type"]
                es_row["summons_number"] = row["summons_number"]
                es_row["issue_date"] = row["issue_date"]
                es_row["violation_time"] = row["violation_time"]
                es_row["violation"] = row["violation"]
                es_row["fine_amount"] = float(row["fine_amount"])
                es_row["penalty_amount"] = float(row["penalty_amount"])
                es_row["interest_amount"] = float(row["interest_amount"])
                es_row["reduction_amount"] = float(row["reduction_amount"])
                es_row["payment_amount"] = float(row["payment_amount"])
                es_row["amount_due"] = float(row["amount_due"])
                es_row["precinct"] = row["precinct"]
                es_row["county"] = row["county"]
                es_row["issuing_agency"] = row["issuing_agency"] 
            except Exception as e:
                print(e)  
                continue
            
            es_rows.append(es_row)
                
                    
        bulk_upload_data = ""
        for i, line in enumerate(es_rows):
            print(f"Handling row {line['summons_number']} {i}")
            action = '{"index": {"_index": "'+INDEX_NAME+'", "_type" : "_doc"}}'
            data = json.dumps(line)
            bulk_upload_data += f"{action}\n"
            bulk_upload_data += f"{data}\n"
            
                # print(bulk_upload_data)
                
                # here we will push to elasticsearch
            try:
                resp = requests.post(
                    f"{ES_HOST}/_bulk",
                    auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD),
                    data=bulk_upload_data,
                    headers={
                    "Content-Type": "application/x-ndjson"
                    }
                )
                resp.raise_for_status()
                print("done")
            except Exception as e:
                print(f"Failed to upload to elasticsearch! {e}") 
