from time import sleep
from datetime import datetime
from decimal import Decimal

from boto3 import client

from common import create_qldb_driver
from pyqldb.driver.qldb_driver import QldbDriver
#from base64 import encode, decode
import json

qldb_client = client('qldb')

def select_document(driver, table_name, username , password):
    """
    Insert the given list of documents into a table in a single transaction.

    :type driver: :py:class:`pyqldb.driver.qldb_driver.QldbDriver`
    :param driver: An instance of the QldbDriver class.

    :type table_name: str
    :param table_name: Name of the table to insert documents into.

    :type documents: list
    :param documents: List of documents to insert.

    :rtype: list
    :return: List of documents IDs for the newly inserted documents.
    """
    print('Validating passed credentials') 
    
    login_status = False
    user_type = ''
    document_id = ''
    
    statement = "SELECT Username, Usertype, Password, MemberDocId FROM {} WHERE Username = '{}'".format(table_name, username)
    cursor = driver.execute_lambda(lambda executor: executor.execute_statement(statement))
    
    for doc in cursor:
        db_username = doc['Username']
        db_password = doc['Password']
        # db_password = doc['Password'].decode('ascii')
        db_memeberid = doc['MemberDocId']
        db_usertype = doc['Usertype']
    
    # validate credentials 
    if db_username == username and db_password == password: #password.decode('ascii'):
        login_status = True
        user_type = db_usertype
        document_id = db_memeberid
        
    
    return login_status, user_type, document_id


def lambda_handler(event, context):

    """
    Process values from Event payload
    """
    
    API_flow = False
    processing_error = False
    return_msg = ''
    response = {}
    
    if event.get('body') is None:
        # pick from lambda test payload
        #print("lambda test flow")
        ledger_name = event.get('ledgername')
        username = event.get('username')
        password = event.get('password')

        #print(ledger_name)
    else:
        API_flow = True
        # pick from API request
        # print("API integration flow")
        body_dict_payload = json.loads(event.get('body'))
        ledger_name = body_dict_payload.get('ledgername')
        username = body_dict_payload.get('username')
        # pick base 64 encoded pwd from payload 
        password = body_dict_payload.get('password')
        # password = body_dict_payload.get('password').encode('ascii')
        # print(ledger_name)

    print('Started processing of login request ..')
    
    
    
    try:
        with create_qldb_driver(ledger_name) as driver:
            
            login_status, user_type , doc_id = select_document(driver,'Member', username ,password)
            
            if login_status:
                
                response = {
                    "isBase64Encoded": "false",
                    "statusCode": 200,
                    "body": json.dumps({
                        "message": "Login success",
                        "Usertype" : user_type,
                        "MemberId" : doc_id
                    })
                }
            else:
                response = {
                    "isBase64Encoded": "false",
                    "statusCode": 403,
                    "body": json.dumps({
                        "message": "Login Failed"
                    })
                }            
            
    except Exception as e:
        print('Error processing request {}',format(e))
        response = {
            "isBase64Encoded": "false",
            "statusCode": 500,
            "body": 'Error processing request - '+ format(e)
        }
    
    return response