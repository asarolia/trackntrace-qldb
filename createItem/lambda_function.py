from time import sleep
from datetime import datetime
from decimal import Decimal
from boto3 import client
from common import convert_object_to_ion, create_qldb_driver, to_base_64
from pyqldb.driver.qldb_driver import QldbDriver
import json



qldb_client = client('qldb')



def insert_transaction_document(driver, table_name, document, statement):
   
    print('First Checking if record exist or not') 
    document_id = ''
    cursor = driver.execute_lambda(lambda executor: executor.execute_statement(statement))
    
    # Check if there is any record in the cursor
    first_record = next(cursor, None)
    
    if first_record:
      
        print("Existing record, updating digest tip...")
        
        q_statement = "UPDATE {} SET Digest = '{}' , DigestBlockAddress = '{}', owner = '{}', version = {} WHERE label = '{}' AND state = '{}' AND data = '{}'".format(table_name, document.get('Digest'), document.get('DigestBlockAddress'), document.get('owner'),document.get('version') , document.get('label'), document.get('state'), document.get('data'))
        print(q_statement)
        cursor = driver.execute_lambda(lambda executor: executor.execute_statement(q_statement))
        for doc in cursor:
            #print(doc)
            document_id = doc['documentId']
            print(document_id)
        
        pass
    else:
        print('Record does not exist')
        print('Inserting documents in the {} table...'.format(table_name))
        statement = 'INSERT INTO {} ?'.format(table_name)
        cursor = driver.execute_lambda(lambda executor: executor.execute_statement(statement,convert_object_to_ion(document)))
        #print('cursor type is - {}'.format(type(cursor)))
        for doc in cursor:
            #print(doc)
            document_id = doc['documentId']
    return document_id

def insert_item_document(driver, table_name, document, fieldname , fieldvalue):
   
    print('First Checking if record exist or not') 
    document_version = 0
    
    statement = "SELECT * FROM {} WHERE {} = '{}'".format(table_name,fieldname, fieldvalue)
    print(statement)
    
    cursor = driver.execute_lambda(lambda executor: executor.execute_statement(statement))
    
    # Check if there is any record in the cursor
    first_record = next(cursor, None)
    
    #print(first_record)

    if first_record:
        # Record already exists, no need to insert
       
        print("Existing record, fetching document ID from commited metadata")
        statement = "SELECT metadata.id, metadata.version FROM _ql_committed_{} AS p WHERE p.data.{} = '{}'".format(table_name,fieldname, fieldvalue)
        print(statement)
        cursor = driver.execute_lambda(lambda executor: executor.execute_statement(statement))
        for doc in cursor:
            #print(doc['documentId'])
            document_id = doc['id']
            document_version = doc['version']
        
        pass
    else:
        print("Record does not exist")
        print('Inserting documents in the {} table...'.format(table_name))
        statement = 'INSERT INTO {} ?'.format(table_name)
        cursor = driver.execute_lambda(lambda executor: executor.execute_statement(statement,convert_object_to_ion(document)))
        #print('cursor type is - {}'.format(type(cursor)))
        
        if next(cursor, None):
            statement = "SELECT metadata.id, metadata.version FROM _ql_committed_{} AS p WHERE p.data.{} = '{}'".format(table_name,fieldname, fieldvalue)
            newcursor = driver.execute_lambda(lambda executor: executor.execute_statement(statement))
            
            for doc in newcursor:
                #print(doc)
                document_id = doc['id']
                document_version = doc['version']
           
    return document_id, document_version
    

def get_digest_result(name):
    """
    Get the digest of a ledger's journal.

    :type name: str
    :param name: Name of the ledger to operate on.

    :rtype: dict
    :return: The digest in a 256-bit hash value and a block address.
    """
    print("Let's get the current digest of the ledger named {}".format(name))
    result = qldb_client.get_digest(Name=name)
    print(result)
    return result


    
    

def lambda_handler(event, context):

    API_flow = False
    processingerror = False
    return_msg = ''
    
    if event.get('body') is None:
        #print("lambda test flow")
        ledger_name = event.get('ledgername')
        manufacturer_id = event.get('item').get('Manufacturer')
        batch_number = event.get('item').get('ItemSpecifications').get('MfgBatchNumber')
        unit_count = event.get('item').get('ItemSpecifications').get('MfgUnitCount')
        
      
    else:
        API_flow = True
        # pick from API request
        # print("API integration flow")
        body_dict_payload = json.loads(event.get('body'))
        ledger_name = body_dict_payload.get('ledgername')
        manufacturer_id = body_dict_payload.get('item').get('Manufacturer')
        batch_number = body_dict_payload.get('item').get('ItemSpecifications').get('MfgBatchNumber')
        unit_count = body_dict_payload.get('item').get('ItemSpecifications').get('MfgUnitCount')


    for i in range(1,unit_count+1):
        item_id = batch_number+"000"+str(i)
        newitem_id = {'ItemId':item_id}
        if API_flow:
            itempayload = body_dict_payload.get('item')
        else:
            itempayload = event.get('item')
        itempayload.update(newitem_id)
        print(itempayload)
        print('Preparing to insert document - {}'.format(i))
        
        try:
            with create_qldb_driver(ledger_name) as driver:
                doc_id, doc_version = insert_item_document(driver,"Item", itempayload, 'ItemId' , item_id)
                print(doc_id)
                print(doc_version)
                
        except Exception as e:
            processingerror = True
            print('Error inserting or updating documents- {}',format(e))
            #raise e
    
    if not processingerror:
        # Get the Tip of ledger digest 
        print("Trying to get digest tip for ledger..")
        
        try:
            
            result = get_digest_result(ledger_name)
            
            if result.get('Digest') is not None:
                digest_bytes = result.get('Digest')
                print(digest_bytes)
                # Encode input in base64
                encoded_digest = to_base_64(digest_bytes)
                print("Encoded digest - {}".format(encoded_digest))

    
            if result.get('DigestTipAddress', {}).get('IonText') is not None:
                digestblock_value = result.get('DigestTipAddress', {}).get('IonText') 
                print(digestblock_value)
            
        except Exception as e:
            processingerror = True
            print('failed to get digest - {}'.format(e))
        # TODO: write code...
    
    if not processingerror:
        # Store the genesis transaction digest 
        print('Preparing to store Genesis transaction')
        
        tran_doc = {
            'label' : 'Genesis',
            'state':'Manufactured',
            'owner':manufacturer_id,
            'data':batch_number,
            'version': doc_version,
            'Digest' : encoded_digest,
            'DigestBlockAddress': digestblock_value
            
        }
            
        try:
            with create_qldb_driver(ledger_name) as driver:
                q_existence = "SELECT * FROM TransactionActivity WHERE label = 'Genesis' AND state = 'Manufactured' AND data = '{}'".format(batch_number)
                doc_id = insert_transaction_document(driver,"TransactionActivity", tran_doc, q_existence)
                print(doc_id)
        except Exception as e:
            print('Error inserting or updating documents- {}',format(e))
        
    
    if not processingerror:
        
        return_msg = "Successfully added Items"
        
    else:
        # TODO implement
        return_msg = "Item Add functionality failed"
        
    response = {
        "isBase64Encoded": "false",
        "statusCode": 200,
        "body": return_msg
    }
    
    return response