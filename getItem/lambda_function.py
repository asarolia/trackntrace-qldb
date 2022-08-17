from time import sleep
from datetime import datetime
from decimal import Decimal
from boto3 import client
from common import convert_object_to_ion, block_address_to_dictionary, value_holder_to_string, create_qldb_driver
from verifier import verify_document
from pyqldb.driver.qldb_driver import QldbDriver
from base64 import encode, decode, b64encode, b64decode
import json
from amazon.ion.simpleion import dumps, loads

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
        
        q_statement = "UPDATE {} SET Digest = '{}' , DigestBlockAddress = '{}', owner = '{}' where label = '{}'".format(table_name, document.get('Digest'), document.get('DigestBlockAddress'), document.get('Owner') , document.get('label'))
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


    
def get_revision(ledger_name, document_id, block_address, digest_tip_address):
    """
    Get the revision data object for a specified document ID and block address.
    Also returns a proof of the specified revision for verification.

    :type ledger_name: str
    :param ledger_name: Name of the ledger containing the document to query.

    :type document_id: str
    :param document_id: Unique ID for the document to be verified, contained in the committed view of the document.

    :type block_address: dict
    :param block_address: The location of the block to request.

    :type digest_tip_address: dict
    :param digest_tip_address: The latest block location covered by the digest.

    :rtype: dict
    :return: The response of the request.
    """
    result = qldb_client.get_revision(Name=ledger_name, BlockAddress=block_address, DocumentId=document_id,
                                      DigestTipAddress=digest_tip_address)
    return result
    

def verify_coldchain(driver, ledger_name, package, batch):
    print('Starting coldchain verification..')
    print('Trying to get the transaction digest for verification - ')
    status = False 
    coldchain_data = package+':'+batch
    statement = "select r.data.Digest AS Digest, r.data.DigestBlockAddress AS DigestBlockAddress, r.data.state, r.data.version from _ql_committed_TransactionActivity r where r.data.label = 'Coldchain' and r.data.data = '{}'".format(coldchain_data)
    print(statement)
    cursor = driver.execute_lambda(lambda executor: executor.execute_statement(statement))
    
    print('Fetched records from Transaction history  ')
    
    for doc in cursor:
        #print(doc)
        encoded_digest = doc['Digest']
        digestblock_address = doc['DigestBlockAddress']
        coldchain_state = doc['state']
        doc_version = doc['version']
        
    # Decode the encoded digest to bytes 
        
    digest_bytes = b64decode(encoded_digest)
    statement = "SELECT r_id AS id FROM _ql_committed_Item AS r BY r_id WHERE r.data.PackageLabel = '{}' AND r.data.ItemSpecifications.MfgBatchNumber = '{}'".format(package, batch)
    cursor = driver.execute_lambda(lambda executor: executor.execute_statement(statement))
    
    for row in cursor:
        print('Trying to get history records for - {}'.format(row['id']))
        # Get the specific version document from QLDB ledger for verification 
        
        q_statement = "SELECT h.blockAddress As blockAddress, h.data.PackageSeal AS PackageSeal FROM history(Item) AS h WHERE h.metadata.id = '{}' AND h.metadata.version = {}".format(row['id'],doc_version)
        newcursor = driver.execute_lambda(lambda executor: executor.execute_statement(q_statement))
        
        block_address = ''
        package_seal = ''
        doccnt = 0
        
        for histrecord in newcursor:
           
            block_address = histrecord['blockAddress']
            print(block_address)
            package_seal = histrecord['PackageSeal']
            print(package_seal)
        
        
        document_id = row['id']
        
        print("Comparing package seal:")
        
        if coldchain_state == package_seal:
            
            result = get_revision(ledger_name, document_id, block_address_to_dictionary(block_address), block_address_to_dictionary(digestblock_address))
        
            revision = result.get('Revision').get('IonText')
            
            document_hash = loads(revision).get('hash')
            
            proof = result.get('Proof')
    
            verified = verify_document(document_hash, digest_bytes, proof)
            if not verified:
                status = False
                return status
            else:
                status = True
        else:
            status = False
            
    return status, coldchain_state
    
    

def verify_batch_compliance(driver, ledger_name, batch):
    status = False 
    statement = "select r.data.Digest AS Digest, r.data.DigestBlockAddress AS DigestBlockAddress, r.data.state, r.data.version from _ql_committed_TransactionActivity r where r.data.label = 'Compliance' and r.data.data = '{}'".format(batch)
    cursor = driver.execute_lambda(lambda executor: executor.execute_statement(statement))
    
    print('Fetched records from Transaction history  ')
    
    for doc in cursor:
        #print(doc)
        encoded_digest = doc['Digest']
        digestblock_address = doc['DigestBlockAddress']
        compliance_state = doc['state']
        doc_version = doc['version']
     
    # Decode the encoded digest to bytes 
        
    digest_bytes = b64decode(encoded_digest)

    statement = "SELECT r_id AS id FROM _ql_committed_Item AS r BY r_id WHERE r.data.ItemSpecifications.MfgBatchNumber = '{}'".format(batch)
    
    cursor = driver.execute_lambda(lambda executor: executor.execute_statement(statement))
    
    for row in cursor:
        print('Trying to get history records for - {}'.format(row['id']))
        # Get the specific version document from QLDB ledger for verification 
        
        q_statement = "SELECT h.blockAddress As blockAddress, h.data.ItemSpecifications.QualityCompliance AS QualityCompliance FROM history(Item) AS h WHERE h.metadata.id = '{}' AND h.metadata.version = {}".format(row['id'],doc_version)
       
        newcursor = driver.execute_lambda(lambda executor: executor.execute_statement(q_statement))
        
        block_address = ''
        item_compliance = ''
        doccnt = 0
        
        for histrecord in newcursor:
            
            block_address = histrecord['blockAddress']
     
            item_compliance = histrecord['QualityCompliance']

        
        document_id = row['id']
        
        print("Comparing compliance:")
        
        if compliance_state == item_compliance:
            
            result = get_revision(ledger_name, document_id, block_address_to_dictionary(block_address), block_address_to_dictionary(digestblock_address))
        
            revision = result.get('Revision').get('IonText')
            
            document_hash = loads(revision).get('hash')
           
            proof = result.get('Proof')
            
    
            verified = verify_document(document_hash, digest_bytes, proof)
            if not verified:
               
                status = False
                return status
            else:
               
                status = True
        else:
            status = False
           
            
    return status, compliance_state
    

def lambda_handler(event, context):
    
    # Read the event paylaod for leldgername , batchnumber, package , activity
    
    API_flow = False
    processingerror = False
    return_message = ''
    verify_status = False
    
    if event.get('body') is None:
        # pick from lambda test payload
        #print("lambda test flow")
        ledger_name = event.get('ledgername')
        type_name = event.get('type')
        type_value = event.get('value')
        #print(ledger_name)
    else:
        API_flow = True
        # pick from API request
        # print("API integration flow")
        # print(type(event.get('body')))
        body_dict_payload = json.loads(event.get('body'))
        ledger_name = body_dict_payload.get('ledgername')
        type_name = body_dict_payload.get('type')
        type_value = body_dict_payload.get('value')
    
    if type_name == 'item':
        
      
        try:
            
            with create_qldb_driver(ledger_name) as driver:

                print("Trying to get details for {} - {}. Processing ...".format(type_name, type_value))
                        
                statement = "select r.data from _ql_committed_Item As r where r.data.ItemId = '{}'".format(type_value)
                print(statement)
                cursor = driver.execute_lambda(lambda executor: executor.execute_statement(statement))
                
                item_details = {}
                
                for doc in cursor:
                   
        
                    item_id = doc['data']['ItemId']
                    item_batch = doc['data']['ItemSpecifications']['MfgBatchNumber']
                    item_quality = doc['data']['ItemSpecifications']['QualityCompliance']
                    item_package = doc['data']['PackageLabel']
                    item_seal = doc['data']['PackageSeal']
                   
                    item_details = doc['data']
                
               
                
                return_message = item_details
                
                verify_status, compliance = verify_batch_compliance(driver, ledger_name, item_batch)
                        
                if verify_status:
                    if compliance == 'PASS':
               
                        quality_message = 'Completed'
                    else:
                        
                        quality_message = 'Not Completed'
                else:
                    quality_message = 'Ledger verification failed for batch :' + item_batch
                    
                
                #Fetch coldchain
                
                verify_status, coldchain_status = verify_coldchain(driver, ledger_name, item_package, item_batch)
                    
                if verify_status:
                    if coldchain_status == '1':
                        coldchain_message = 'Not Broken'
                    else:
                        coldchain_message = 'Broken'
                else:
                    coldchain_message = 'Ledger verification failed for package: ' + item_package + ' and batch :' + item_batch +'; '
                    
                
        except Exception as e:
            processingerror = True
            print('Server processing failed during complaince verification due to unexpected error - {}',format(e))
            return_message = 'Server processing failed during complaince verification due to unexpected error - {}',format(e)
  
   
   
    if not processingerror:
        
        # TODO implement
        return {
            "isBase64Encoded": False,
            "statusCode": 200,
            "body": {"Data": str(return_message), "QualityCompliance": quality_message, "Coldchain": coldchain_message}
        }
    else:
        # TODO implement
        return {
            "statusCode": 503,
            "isBase64Encoded": False,
            "body": return_message
        }