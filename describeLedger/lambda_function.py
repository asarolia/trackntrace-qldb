from time import sleep
from datetime import datetime
from decimal import Decimal
from boto3 import client
from pyqldb.driver.qldb_driver import QldbDriver
import json

qldb_client = client('qldb')

ACTIVE_STATE = "ACTIVE"

def get_ledger_state(ledger_name):
    ledger_status = ''
    ledger_exist = False
    result = ''
    try:
        
        result = qldb_client.describe_ledger(Name=ledger_name)
        if result.get('State') == ACTIVE_STATE:
            ledger_exist = True
            ledger_status = 'ACTIVE'
        else:
            ledger_exist = True
            ledger_status = 'NOT_ACTIVE'
        
    except Exception as e:
        message = '' + str(e)
        if "ResourceNotFoundException" in message:
            ledger_exist = False
        else:
            print(e)
        
    return ledger_exist,ledger_status
    

def lambda_handler(event, context):
    
    API_flow = False
    processing_error = False
    return_msg = ''
    
    if event.get('body') is None:
        # Lambda flow
        ledger_name = event.get('ledgername')
        #print(ledger_name)
    else:
        # API Gateway flow
        API_flow = True
        body_dict_payload = json.loads(event.get('body'))
        ledger_name = body_dict_payload.get('ledgername')
        # print(ledger_name)
    
    
    
    # Check if ledger exist or not
    ledger_exist, ledger_status = get_ledger_state(ledger_name)
    
    if ledger_exist:
        if ledger_status == 'ACTIVE':
            return_msg = 'Ledger is active now and ready for further operation'
        else:
            return_msg = 'Ledger creation in process.. Please wait!'
    else:
        return_msg = 'Ledger with name - {} not exist. Please create new'.format(ledger_name)
        

    response = {
        "isBase64Encoded": "false",
        "statusCode": 200,
        "body": return_msg
    }
        
    
    return response
        