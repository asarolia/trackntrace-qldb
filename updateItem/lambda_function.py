from time import sleep
from datetime import datetime
from decimal import Decimal
from boto3 import client
from common import convert_object_to_ion, create_qldb_driver, to_base_64
from pyqldb.driver.qldb_driver import QldbDriver
import json



qldb_client = client('qldb')

def insert_update_transaction_document(driver, table_name, document, statement):
   
    print('First Checking if record exist or not') 
    document_id = ''
    
    cursor = driver.execute_lambda(lambda executor: executor.execute_statement(statement))
    
    # Check if there is any record in the cursor
    first_record = next(cursor, None)
    
    #print(first_record)

    if first_record:
      
        print("Existing record, updating digest tip...")
        
        q_statement = "UPDATE {} SET Digest = '{}' , DigestBlockAddress = '{}', owner = '{}', version = {}, state = '{}' WHERE label = '{}' AND data = '{}'".format(table_name, document.get('Digest'), document.get('DigestBlockAddress'), document.get('owner'),document.get('version') , document.get('state'), document.get('label'), document.get('data'))
        print(q_statement)
        cursor = driver.execute_lambda(lambda executor: executor.execute_statement(q_statement))
        for doc in cursor:
            #print(doc)
            document_id = doc['documentId']
            print(document_id)
        
    else:
        print('Record does not exist')
        print('Inserting documents in the {} table...'.format(table_name))
        statement = 'INSERT INTO {} ?'.format(table_name)
        cursor = driver.execute_lambda(lambda executor: executor.execute_statement(statement,convert_object_to_ion(document)))
        #print('cursor type is - {}'.format(type(cursor)))
        for doc in cursor:
            #print(doc)
            document_id = doc['documentId']
            print(document_id)
            
       
    return document_id
    

def update_item_compliance(driver, table_name, document, fieldname , fieldvalue, data):
   
    print('First Checking if record exist or not') 
    
    statement = "SELECT metadata.id FROM _ql_committed_{} As p WHERE p.data.ItemSpecifications.QualityCompliance = '' AND p.data.ItemSpecifications.{} = '{}'".format(table_name,fieldname, fieldvalue)
    print(statement)
    #cursor = driver.execute_lambda(lambda executor: executor.execute_statement(statement,convert_object_to_ion(fieldvalue)))
    cursor = driver.execute_lambda(lambda executor: executor.execute_statement(statement))
    
    # Check if there is any record in the cursor
    first_record = next(cursor, None)
    doccount = 0
    version = 0
    #print(first_record)

    if first_record:
        
        print("Existing record found, initiating document update processing..")
        statement = "UPDATE {} As u SET u.ItemSpecifications.QualityCompliance = '{}' WHERE u.ItemSpecifications.MfgBatchNumber = '{}'".format(table_name,data,fieldvalue)
        print(statement)
        cursor = driver.execute_lambda(lambda executor: executor.execute_statement(statement))
        doccount = 0
        for doc in cursor:
            #print(doc['documentId'])
            document_id = doc['documentId']
            doccount = doccount + 1
        
        print("Trying to get the revision number")
        
        statement = "SELECT metadata.version FROM _ql_committed_{} As p WHERE p.data.ItemSpecifications.QualityCompliance = '{}' AND p.data.ItemSpecifications.{} = '{}'".format(table_name, data ,fieldname, fieldvalue)
        print(statement)
        #cursor = driver.execute_lambda(lambda executor: executor.execute_statement(statement,convert_object_to_ion(fieldvalue)))
        cursor = driver.execute_lambda(lambda executor: executor.execute_statement(statement)) 
        
        # Check if there is any record in the cursor
        next_record = next(cursor, None)
        count = 0
        version = 0
        if next_record:
            for doc in cursor:
                version = doc['version']
                break
        
    else:
        print("No existing record found for compliance update!")
        
       
    
    return doccount, version
    
def update_item_coldchain(driver,table_name, document, fieldname , fieldvalue, data):
    print('First Checking if record exist or not') 
    
    statement = "SELECT metadata.id FROM _ql_committed_{} As p WHERE p.data.{} = '{}' AND p.data.ItemSpecifications.MfgBatchNumber = '{}'".format(table_name,fieldname, fieldvalue, document.get('batch'))
    print(statement)
    #cursor = driver.execute_lambda(lambda executor: executor.execute_statement(statement,convert_object_to_ion(fieldvalue)))
    cursor = driver.execute_lambda(lambda executor: executor.execute_statement(statement))
    
    # Check if there is any record in the cursor
    first_record = next(cursor, None)
    doccount = 0
    version = 0
    #print(first_record)

    if first_record:
        
        print("Existing record found, initiating document update processing..")
        statement = "UPDATE {} As u SET u.PackageSeal = '{}', u.PkgOwner = '{}' WHERE u.PackageLabel = '{}' AND u.ItemSpecifications.MfgBatchNumber = '{}'".format(table_name,data, document.get('memberid'),fieldvalue, document.get('batch'))
        print(statement)
        cursor = driver.execute_lambda(lambda executor: executor.execute_statement(statement))
        doccount = 0
        for doc in cursor:
            #print(doc['documentId'])
            document_id = doc['documentId']
            doccount = doccount + 1
            
        print(doccount)
        print("Trying to get the revision number")
        
        statement = "SELECT metadata.version FROM _ql_committed_{} As p WHERE p.data.{} = '{}' AND p.data.ItemSpecifications.MfgBatchNumber = '{}'".format(table_name,fieldname, fieldvalue, document.get('batch'))
        print(statement)
        #cursor = driver.execute_lambda(lambda executor: executor.execute_statement(statement,convert_object_to_ion(fieldvalue)))
        cursor = driver.execute_lambda(lambda executor: executor.execute_statement(statement)) 
        
        # Check if there is any record in the cursor
        next_record = next(cursor, None)
        version = 0
        if next_record:
            for doc in cursor:
                version = doc['version']
                print(version)
                break
    else:
        print("No existing record found for update!")
        
       
    
    return doccount, version

def get_packaging_version(driver,table_name, document):
    statement = "SELECT metadata.version FROM _ql_committed_{} As p WHERE p.data.PackageLabel = '{}' AND p.data.ItemSpecifications.MfgBatchNumber = '{}'".format(table_name,document.get('package'), document.get('batch'))
    print(statement)
    #cursor = driver.execute_lambda(lambda executor: executor.execute_statement(statement,convert_object_to_ion(fieldvalue)))
    cursor = driver.execute_lambda(lambda executor: executor.execute_statement(statement))
    
    # Check if there is any record in the cursor
    next_record = next(cursor, None)
    count = 0
    version = 0
    if next_record:
        for doc in cursor:
            version = doc['version']
            break
    
    return version
    

def update_item_packaging(driver, table_name, document, fieldname , fieldvalue):
   
    print('First Checking if record exist or not') 
    
    statement = "SELECT metadata.id FROM _ql_committed_{} As p WHERE p.data.{} = '{}'".format(table_name,fieldname, fieldvalue)
    print(statement)
    #cursor = driver.execute_lambda(lambda executor: executor.execute_statement(statement,convert_object_to_ion(fieldvalue)))
    cursor = driver.execute_lambda(lambda executor: executor.execute_statement(statement))
    
    # Check if there is any record in the cursor
    first_record = next(cursor, None)
    doccount = 0
    version = 0
    #print(first_record)

    if first_record:
        
        print("Existing record found, initiating document update processing..")
        statement = "UPDATE {} As u SET u.PackageLabel = '{}', u.PkgOwner = '{}' WHERE u.ItemId = '{}' AND u.ItemSpecifications.MfgBatchNumber = '{}'".format(table_name,document.get('package'),document.get('memberid') ,fieldvalue, document.get('batch'))
        print(statement)
        cursor = driver.execute_lambda(lambda executor: executor.execute_statement(statement))
        doccount = 0
        for doc in cursor:
            #print(doc['documentId'])
            document_id = doc['documentId']
            doccount = doccount + 1
        
        print("Trying to get the revision number")
        
        statement = "SELECT metadata.version FROM _ql_committed_{} As p WHERE p.data.{} = '{}'".format(table_name,fieldname, fieldvalue)
        print(statement)
        #cursor = driver.execute_lambda(lambda executor: executor.execute_statement(statement,convert_object_to_ion(fieldvalue)))
        cursor = driver.execute_lambda(lambda executor: executor.execute_statement(statement)) 
        
        # Check if there is any record in the cursor
        next_record = next(cursor, None)
        count = 0
        version = 0
        if next_record:
            for doc in cursor:
                version = doc['version']
                break
            
    else:
        print("No existing record found for update!")
        
       
    
    return doccount, version

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
    
    # Read the event paylaod for leldgername , batchnumber, package , label, usertype, memberid, data
    
    API_flow = False
    processingerror = False
    return_msg = ''
    
    if event.get('body') is None:
        # pick from lambda test payload
        #print("lambda test flow")
        ledger_name = event.get('ledgername')
        batch = event.get('batch')
        package = event.get('package')
        user_type = event.get('usertype')
        member_id = event.get('memberid')
        data = event.get('data')
        activity = event.get('activity')
        #print(ledger_name)
    else:
        API_flow = True
        # pick from API request
        # print("API integration flow")
        # print(type(event.get('body')))
        body_dict_payload = json.loads(event.get('body'))
        ledger_name = body_dict_payload.get('ledgername')
        batch = body_dict_payload.get('batch')
        package = body_dict_payload.get('package')
        user_type = body_dict_payload.get('usertype')
        member_id = body_dict_payload.get('memberid')
        data = body_dict_payload.get('data')
        activity = body_dict_payload.get('activity')
        # print(ledger_name)
    
    
    if activity == 'Compliance':
        doc_cnt = 0
        try:
            with create_qldb_driver(ledger_name) as driver:
                if API_flow:
                    doc_cnt, version = update_item_compliance(driver,"Item", body_dict_payload, 'MfgBatchNumber' , batch, data)
                else:
                    doc_cnt, version = update_item_compliance(driver,"Item", event, 'MfgBatchNumber' , batch, data)
                print('Updated compliance for {} documents'.format(doc_cnt))
            
            # sleep for 3 seconds
            sleep(2)
            
            print("Trying to get digest tip for ledger for later verification..")
            result = get_digest_result(ledger_name)
    
            if result.get('Digest') is not None:
                digest_bytes = result.get('Digest')
                print(digest_bytes)
                # Encode input in base64
                encoded_digest = to_base_64(digest_bytes)
                print("Encoded digest - {}".format(encoded_digest))
    
    
            if result.get('DigestTipAddress', {}).get('IonText') is not None:
                digestblock_value = result.get('DigestTipAddress', {}).get('IonText') #value_holder_to_string(digest_response['DigestTipAddress']['IonText'])
                print(digestblock_value)
            
            print('Preparing to store compliance transaction')
    
            tran_doc = {
                'label' : 'Compliance',
                'state':data,
                'owner':member_id,
                'data':batch,
                'version': version,
                'Digest' : encoded_digest,
                'DigestBlockAddress': digestblock_value
                
            }
            
            with create_qldb_driver(ledger_name) as driver1:
                    
                q_existence = "SELECT * FROM TransactionActivity WHERE label = 'Compliance' AND state = '{}' AND data = '{}'".format(data, batch)
                doc_id = insert_update_transaction_document(driver1,"TransactionActivity", tran_doc, q_existence)
                print(doc_id)
            
            return_msg = 'Compliance successfully updated'
                        
        except Exception as e:
            processingerror = True
            print('Error inserting or updating documents- {}'.format(e))
            return_msg = 'Error inserting or updating documents- {}'.format(e)
            #raise e
            
    
    if activity == 'Package':
        
        shipment_data = package + ':' + batch
        print(shipment_data)
        # Check if update is for package shipment
        if len(data) > 0:
            if data == 'Shipment':
                print('Processing shipment record to be used for ledger verification later on')
                # First get the document revision
                try:
                    with create_qldb_driver(ledger_name) as driver:
                        if API_flow:
                            version = get_packaging_version(driver,"Item", body_dict_payload)
                        else:
                            version = get_packaging_version(driver,"Item", event)
                    # sleep for 2 seconds
                    sleep(2)
                    
                    print("Trying to get digest tip for ledger for later verification..")
                    result = get_digest_result(ledger_name)
            
                    if result.get('Digest') is not None:
                        digest_bytes = result.get('Digest')
                        print(digest_bytes)
                        # Encode input in base64
                        encoded_digest = to_base_64(digest_bytes)
                        print("Encoded digest - {}".format(encoded_digest))
            
            
                    if result.get('DigestTipAddress', {}).get('IonText') is not None:
                        digestblock_value = result.get('DigestTipAddress', {}).get('IonText') #value_holder_to_string(digest_response['DigestTipAddress']['IonText'])
                        print(digestblock_value)
                    
                    print('Preparing to store Shipment transaction')
            
                    tran_doc = {
                        'label' : 'Package',
                        'state':'Shipment',
                        'owner':member_id,
                        'data':shipment_data,
                        'version':version,
                        'Digest' : encoded_digest,
                        'DigestBlockAddress': digestblock_value
                        
                    }
                    
                    with create_qldb_driver(ledger_name) as driver:
                    
                        q_existence = "SELECT * FROM TransactionActivity WHERE label = 'Package' AND state = 'Shipment' AND data = '{}'".format(shipment_data)
                        doc_id = insert_update_transaction_document(driver,"TransactionActivity", tran_doc, q_existence)
                        
                    if len(doc_id) > 0:
                        return_msg = 'Shipment successfully recorded'
                    else:
                        return_msg = 'Failed to process shipment record'
                        
                    
                                
                except Exception as e:
                    processingerror = True
                    print('Error inserting or updating documents- {}'.format(e))
                    return_msg = 'Error inserting or updating documents- {}'.format(e)
                    #raise e
                
            else:
                # update the Item record
                try:
                    with create_qldb_driver(ledger_name) as driver:
                        if API_flow:
                            doc_cnt, version = update_item_packaging(driver,"Item", body_dict_payload, 'ItemId' , data)
                        else:
                            
                            doc_cnt, version = update_item_packaging(driver,"Item", event, 'ItemId' , data)
                        print('Updated packaging for {} documents'.format(doc_cnt))
                    
                    if doc_cnt > 0:
                        return_msg = 'Item packaging details successfully updated'
                    else:
                        return_msg = 'Existing Item record not found for package update'
                        
                    
                                
                except Exception as e:
                    processingerror = True
                    print('Error inserting or updating documents- {}'.format(e))
                    return_msg = 'Error inserting or updating documents- {}'.format(e)
                    #raise e
                        
            
    if activity == 'Coldchain':
        
        doc_cnt = 0
        coldchain_data = package + ':' + batch
        print(coldchain_data)
        
        try:
            with create_qldb_driver(ledger_name) as driver:
                if API_flow:
                    
                    doc_cnt, version = update_item_coldchain(driver,"Item", body_dict_payload, 'PackageLabel' , package, data)
                else:
                    doc_cnt, version = update_item_coldchain(driver,"Item", event, 'PackageLabel' , package, data)
                    
                print('Updated coldchain for {} documents'.format(doc_cnt))
            
            sleep(2)
            
            print("Trying to get digest tip for ledger for later verification..")
            result = get_digest_result(ledger_name)
    
            if result.get('Digest') is not None:
                digest_bytes = result.get('Digest')
                print(digest_bytes)
                # Encode input in base64
                encoded_digest = to_base_64(digest_bytes)
                print("Encoded digest - {}".format(encoded_digest))
    
    
            if result.get('DigestTipAddress', {}).get('IonText') is not None:
                digestblock_value = result.get('DigestTipAddress', {}).get('IonText') #value_holder_to_string(digest_response['DigestTipAddress']['IonText'])
                print(digestblock_value)
            
            print('Preparing to store Coldchain transaction')
    
            tran_doc = {
                'label' : 'Coldchain',
                'state':data,
                'owner':member_id,
                'data':coldchain_data,
                'version': version,
                'Digest' : encoded_digest,
                'DigestBlockAddress': digestblock_value
                
            }
            
            with create_qldb_driver(ledger_name) as driver1:
                    
                q_existence = "SELECT * FROM TransactionActivity WHERE label = 'Coldchain' AND data = '{}'".format(coldchain_data)
                print(q_existence)
                doc_id = insert_update_transaction_document(driver1,"TransactionActivity", tran_doc, q_existence)
                print(doc_id)
            
            if len(doc_id) > 0:
                return_msg = 'Package coldchain successfully updated'
                
            else:
                
                return_msg = 'Some error occurred during coldchain ledger activity'
                        
        except Exception as e:
            processingerror = True
            print('Error inserting or updating coldchain - {}'.format(e))
            return_msg = 'Error inserting or updating coldchain - {}'.format(e)
            #raise e

    
   
    response = {
        "isBase64Encoded": "false",
        "statusCode": 200,
        # "headers": { "headerName": "headerValue", ... },
        "body": return_msg
    }
    
    return response