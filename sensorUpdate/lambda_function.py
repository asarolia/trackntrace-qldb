from time import sleep
from datetime import datetime
from decimal import Decimal
from boto3 import client
from common import convert_object_to_ion, to_base_64, create_qldb_driver
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
        
        q_statement = "UPDATE {} SET Digest = '{}' , DigestBlockAddress = '{}', owner = '{}', version = {}, state = '{}'  WHERE label = '{}' AND data = '{}'".format(table_name, document.get('Digest'), document.get('DigestBlockAddress'), document.get('owner'), document.get('version'), document.get('state') , document.get('label'), document.get('data'))

        cursor = driver.execute_lambda(lambda executor: executor.execute_statement(q_statement))
        for doc in cursor:
            #print(doc)
            document_id = doc['documentId']
           
        
    else:
        print('Record does not exist')
        
        statement = 'INSERT INTO {} ?'.format(table_name)
        cursor = driver.execute_lambda(lambda executor: executor.execute_statement(statement,convert_object_to_ion(document)))
        
        for doc in cursor:
         
            document_id = doc['documentId']
            print(document_id)
            
       
    return document_id
    

def update_items_with_coldchain(driver,table_name, document, fieldname , fieldvalue):
    print('First Checking if record exist or not') 
    
    statement = "SELECT metadata.id , data.Storage.MaxTemperature, data.Storage.MinTemperature, data.PkgOwner, data.PackageSeal FROM _ql_committed_{} As p WHERE p.data.{} = '{}' AND p.data.ItemSpecifications.MfgBatchNumber = '{}'".format(table_name,fieldname, fieldvalue, document.get('batch'))
    
    cursor = driver.execute_lambda(lambda executor: executor.execute_statement(statement))
    
    # Check if there is any record in the cursor
    first_record = next(cursor, None)
    doccount = 0
    sensor_temp = document.get('temperature')
    return_msg = ''
   

    if first_record:
        
        
        print("Existing record found, initiating document update processing..")
        
        for eachrow in cursor:
            doc_id = eachrow['id']
            max_temp = eachrow['MaxTemperature']
            min_temp = eachrow['MinTemperature']
            pkg_owner = eachrow['PkgOwner']
            pkg_seal = eachrow['PackageSeal']
            
            if sensor_temp > max_temp or sensor_temp < min_temp:
                # item coldchain is broken, update document seal & transaction ledger
                pkg_seal = 0
                statement = "UPDATE {} As u SET u.PackageSeal = '{}' WHERE u.PackageLabel = '{}' AND u.PkgOwner = '{}' AND u.ItemSpecifications.MfgBatchNumber = '{}'".format(table_name,pkg_seal, fieldvalue,pkg_owner,document.get('batch'))
                print(statement)
                cursor = driver.execute_lambda(lambda executor: executor.execute_statement(statement))
                
                if next(cursor, None):
                    print('Updated package seal for - {}'.format(doc_id))
                
                print("Trying to get the revision number")
                
                sleep(3)
        
                statement = "SELECT metadata.version FROM _ql_committed_{} As p WHERE p.data.PackageLabel = '{}' AND p.data.ItemSpecifications.MfgBatchNumber = '{}' AND p.data.PackageSeal = '{}' AND p.data.PkgOwner = '{}'".format(table_name,fieldvalue,document.get('batch'), pkg_seal, pkg_owner)
             
                
                cursor = driver.execute_lambda(lambda executor: executor.execute_statement(statement)) 
                
                # Check if there is any record in the cursor
                #next_record = next(cursor, None)
                count = 0
                version = 0
                for doc in cursor:
                    version = doc['version']
                    print(version)
                    break
                
                # Read and store transaction ledger entry 
                
                print("Trying to get digest tip for ledger for later verification..")
                result = get_digest_result(document.get('ledgername'))
        
                if result.get('Digest') is not None:
                    digest_bytes = result.get('Digest')
                    
                    # Encode input in base64
                    encoded_digest = to_base_64(digest_bytes)

                if result.get('DigestTipAddress', {}).get('IonText') is not None:
                    digestblock_value = result.get('DigestTipAddress', {}).get('IonText') #value_holder_to_string(digest_response['DigestTipAddress']['IonText'])
       
                
                print('Preparing to store Coldchain transaction')
                coldchain_data = document.get('package')+':'+document.get('batch')
        
                tran_doc = {
                    'label' : 'Coldchain',
                    'state':pkg_seal,
                    'owner':pkg_owner,
                    'data':coldchain_data,
                    'version' : version,
                    'Digest' : encoded_digest,
                    'DigestBlockAddress': digestblock_value
                    
                }
                
                q_existence = "SELECT * FROM TransactionActivity WHERE label = 'Coldchain' AND data = '{}'".format(coldchain_data)
                doc_id = insert_update_transaction_document(driver,"TransactionActivity", tran_doc, q_existence)
                
                if len(doc_id) > 0:
                    print('Package coldchain successfully updated') 
                    
                else:
                    
                    print('Some error occurred during coldchain ledger activity')
                        
            print("Temperature in permissible range, hence skipped update!")
    else:
        print("No existing record found!")
        
    
    

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
    
    # Read the event paylaod from IoT sensor
    print(event)
    
    processingerror = False
    return_msg = ''
    ledger_name = event.get('ledgername')
    batch = event.get('batch')
    package = event.get('package')
    temp_reading = event.get('temperature')

    
    try:
        with create_qldb_driver(ledger_name) as driver:
            update_items_with_coldchain(driver,"Item", event, 'PackageLabel' , package)
            
        print('All processing successfully completed')
    
    except Exception as e:
            processingerror = True
            print('Error inserting or updating documents- {}'.format(e))
            
    