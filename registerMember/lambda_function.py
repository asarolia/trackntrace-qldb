from time import sleep
from datetime import datetime
from decimal import Decimal
from boto3 import client
from common import convert_object_to_ion, get_document_ids, get_document_ids_from_dml_results, print_result, create_qldb_driver, getTableMapping
from pyqldb.driver.qldb_driver import QldbDriver
from base64 import encode, decode
import json

qldb_client = client('qldb')



def insert_document(driver, table_name, document, fieldname , fieldvalue):
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
    print('First Checking if record exist or not') 
    
    statement = "SELECT * FROM {} WHERE {} = '{}'".format(table_name,fieldname, fieldvalue)
    
    cursor = driver.execute_lambda(lambda executor: executor.execute_statement(statement))
    
    # Check if there is any record in the cursor
    first_record = next(cursor, None)
    
    print(first_record)

    if first_record:
        # Record already exists, no need to insert
        statement = "SELECT metadata.id AS documentId FROM _ql_committed_{} AS p WHERE p.data.{} = '{}'".format(table_name,fieldname, fieldvalue)
        cursor = driver.execute_lambda(lambda executor: executor.execute_statement(statement))
        for doc in cursor:
            document_id = doc['documentId']

        pass
    else:
       
        statement = 'INSERT INTO {} ?'.format(table_name)
        cursor = driver.execute_lambda(lambda executor: executor.execute_statement(statement,convert_object_to_ion(document)))

        for doc in cursor:
            #print(doc)
            document_id = doc['documentId']

    return document_id


def lambda_handler(event, context):
    
    """
    Process values from Event payload
    """
    
    API_flow = False
    processing_error = False
    return_msg = ''
    
    if event.get('body') is None:
        # pick from lambda test payload
        #print("lambda test flow")
        ledger_name = event.get('ledgername')
        email = event.get('email')
        username = event.get('username')
        # pick base64 encoded value from payload
        password = event.get('password')
        # password = event.get('password').encode('ascii')
        usertype = event.get('usertype')
        name = event.get('name')
        address = event.get('address')
        licensenumber = event.get('licensenumber')
        registrationnumber = event.get('registrationnumber')
        #print(ledger_name)
    else:
        API_flow = True
        # pick from API request
        # print("API integration flow")
        body_dict_payload = json.loads(event.get('body'))
        ledger_name = body_dict_payload.get('ledgername')
        email = body_dict_payload.get('email')
        username = body_dict_payload.get('username')
        # pick base 64 encoded pwd from payload 
        password = body_dict_payload.get('password')
        # password = body_dict_payload.get('password').encode('ascii')
        usertype = body_dict_payload.get('usertype')
        name = body_dict_payload.get('name')
        address = body_dict_payload.get('address')
        licensenumber = body_dict_payload.get('licensenumber')
        registrationnumber = body_dict_payload.get('registrationnumber')
        # print(ledger_name)

    
 

    """
    Insert documents into a table in a QLDB ledger.
    """
    print('Creating payload for ledger insert..')
    
    doc_m = {
            'Name' : name,
            'Address' : address,
            'LicenseNumber' : licensenumber,
            'RegistrationNumber' : registrationnumber
        }
    
    
   
    print('Preparing to insert document ...')
    try:
        with create_qldb_driver(ledger_name) as driver:
            
            doc_id = insert_document(driver,getTableMapping(usertype), doc_m, 'RegistrationNumber' ,registrationnumber)
          
            doc_mem = {
                'Username' : username,
                'Email' : email,
                'Usertype' : usertype,
                'Password' : password,
                'MemberDocId' : doc_id
            }
            
            doc_id = insert_document(driver,'Member', doc_mem, 'Email',email)
            
            
    except Exception as e:
        processing_error = True
        print('Error inserting or updating documents- {}'.format(e))
        return_msg = 'Error inserting or updating documents- {}'.format(e)
        

    if not processing_error:
        return_msg = "Member registration completed successfully"
    
    response = {
        "isBase64Encoded": "false",
        "statusCode": 200,
        "body": return_msg
    }
    
    return response