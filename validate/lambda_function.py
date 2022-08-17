from time import sleep
from datetime import datetime
from decimal import Decimal
from boto3 import client
from common import convert_object_to_ion, block_address_to_dictionary, create_qldb_driver
from verifier import verify_document
from pyqldb.driver.qldb_driver import QldbDriver
from base64 import encode, decode, b64encode, b64decode
import json
from amazon.ion.simpleion import loads




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
        
    print(encoded_digest)
    print(digestblock_address)
    print(coldchain_state)
    print(doc_version)
        
    # Decode the encoded digest to bytes 
        
    digest_bytes = b64decode(encoded_digest)
    print('Decoded digest bytes - {}'.format(digest_bytes))
    
    print('Fetching all items under package - {} and batch - {}, and verifying coldchain on each one by one ..'.format(package, batch))
    
    statement = "SELECT r_id AS id FROM _ql_committed_Item AS r BY r_id WHERE r.data.PackageLabel = '{}' AND r.data.ItemSpecifications.MfgBatchNumber = '{}'".format(package, batch)
    print(statement)
    cursor = driver.execute_lambda(lambda executor: executor.execute_statement(statement))
    
    for row in cursor:
        print('Trying to get history records for - {}'.format(row['id']))
        # Get the specific version document from QLDB ledger for verification 
        
        q_statement = "SELECT h.blockAddress As blockAddress, h.data.PackageSeal AS PackageSeal FROM history(Item) AS h WHERE h.metadata.id = '{}' AND h.metadata.version = {}".format(row['id'],doc_version)
        print(q_statement)
        newcursor = driver.execute_lambda(lambda executor: executor.execute_statement(q_statement))
        
        block_address = ''
        package_seal = ''
        doccnt = 0
        
        for histrecord in newcursor:
            # doccnt = doccnt + 1
            # print(doccnt)
            # print(histrecord['blockAddress'])
            # print(histrecord['QualityCompliance'])
            block_address = histrecord['blockAddress']
            print(block_address)
            package_seal = histrecord['PackageSeal']
            print(package_seal)
        
        
        document_id = row['id']
        
        print("Comparing package seal:")
        
        if coldchain_state == package_seal:
            
            result = get_revision(ledger_name, document_id, block_address_to_dictionary(block_address), block_address_to_dictionary(digestblock_address))
        
            revision = result.get('Revision').get('IonText')
            print('revision - {}'.format(revision))
            document_hash = loads(revision).get('hash')
            print(type(document_hash))
            print('document hash - {}'.format(document_hash))
    
            proof = result.get('Proof')
            print('Got back a proof: {}.'.format(proof))
    
            verified = verify_document(document_hash, digest_bytes, proof)
            if not verified:
                print('Document revision is not verified, data compromised - {}'.format(document_id))
                print('Aborting rest of the verification chain..')
                status = False
                return status
            else:
                print('Success! Coldchain verified for document - {}'. format(document_id))
                status = True
        else:
            status = False
            print("Item package seal and transaction state does not match!")
            
    return status, coldchain_state
    
    

def verify_batch_compliance(driver, ledger_name, batch):
    print('Starting batch compliance verification..')
    print('Trying to get the transaction digest for verification - ')
    status = False 
    statement = "select r.data.Digest AS Digest, r.data.DigestBlockAddress AS DigestBlockAddress, r.data.state, r.data.version from _ql_committed_TransactionActivity r where r.data.label = 'Compliance' and r.data.data = '{}'".format(batch)
    print(statement)
    cursor = driver.execute_lambda(lambda executor: executor.execute_statement(statement))
    
    print('Fetched records from Transaction history  ')
    
    for doc in cursor:
        #print(doc)
        encoded_digest = doc['Digest']
        digestblock_address = doc['DigestBlockAddress']
        compliance_state = doc['state']
        doc_version = doc['version']
        
    print(encoded_digest)
    print(digestblock_address)
    print(compliance_state)
    print(doc_version)
        
    # Decode the encoded digest to bytes 
        
    digest_bytes = b64decode(encoded_digest)
    print('Decoded digest bytes - {}'.format(digest_bytes))
    
    print('Fetching all items under batch - {}, and verifying complaince on each one by one ..'.format(batch))
    
    statement = "SELECT r_id AS id FROM _ql_committed_Item AS r BY r_id WHERE r.data.ItemSpecifications.MfgBatchNumber = '{}'".format(batch)
    print(statement)
    cursor = driver.execute_lambda(lambda executor: executor.execute_statement(statement))
    
    for row in cursor:
        print('Trying to get history records for - {}'.format(row['id']))
        # Get the specific version document from QLDB ledger for verification 
        
        q_statement = "SELECT h.blockAddress As blockAddress, h.data.ItemSpecifications.QualityCompliance AS QualityCompliance FROM history(Item) AS h WHERE h.metadata.id = '{}' AND h.metadata.version = {}".format(row['id'],doc_version)
        print(q_statement)
        newcursor = driver.execute_lambda(lambda executor: executor.execute_statement(q_statement))
        
        block_address = ''
        item_compliance = ''
        doccnt = 0
        
        for histrecord in newcursor:
            # doccnt = doccnt + 1
            # print(doccnt)
            # print(histrecord['blockAddress'])
            # print(histrecord['QualityCompliance'])
            block_address = histrecord['blockAddress']
            print(block_address)
            item_compliance = histrecord['QualityCompliance']
            print(item_compliance)
        
        
        document_id = row['id']
        
        print("Comparing compliance:")
        
        if compliance_state == item_compliance:
            
            result = get_revision(ledger_name, document_id, block_address_to_dictionary(block_address), block_address_to_dictionary(digestblock_address))
        
            revision = result.get('Revision').get('IonText')
            print('revision - {}'.format(revision))
            document_hash = loads(revision).get('hash')
            print(type(document_hash))
            print('document hash - {}'.format(document_hash))
    
            proof = result.get('Proof')
            print('Got back a proof: {}.'.format(proof))
    
            verified = verify_document(document_hash, digest_bytes, proof)
            if not verified:
                print('Document revision is not verified, data compromised - {}'.format(document_id))
                print('Aborting rest of the verification chain..')
                status = False
                return status
            else:
                print('Success! Compliance verified for document - {}'. format(document_id))
                status = True
        else:
            status = False
            print("Item compliance and transaction state does not match!")
            
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
        verify_activity = event.get('verify')
        #print(ledger_name)
    else:
        API_flow = True
        # pick from API request
        # print("API integration flow")
        # print(type(event.get('body')))
        body_dict_payload = json.loads(event.get('body'))
        ledger_name = body_dict_payload.get('ledgername')
        verify_activity = body_dict_payload.get('verify')
    
    if verify_activity == 'Compliance':
        
        if API_flow:
            batches = body_dict_payload.get('data').get('batch')
        else:
            batches = event.get('data').get('batch')
        try:
            
            with create_qldb_driver(ledger_name) as driver:
            
                for eachbatch in batches:
    
                    print("Verifying ledger for Quality compliance on batch - {}. Processing ...".format(eachbatch))
                            
                    verify_status, compliance = verify_batch_compliance(driver, ledger_name, eachbatch)
                        
                    if verify_status:
                        if compliance == 'PASS':
                            
                            #return_message = return_message + 'Verified ledger quality control check -' + eachbatch +' : '+ 'Completed' +';'
                            return_message = ''+ eachbatch + ': Completed '
                        else:
                            #return_message = return_message + 'Verified ledger quality control check -' + eachbatch +' : '+ 'Not Completed' +';'
                            return_message = ''+ eachbatch + ': Not Completed '
                    else:
                        return_message = return_message + 'Ledger verification failed for batch :' + eachbatch +'; '
                        
        except Exception as e:
            processingerror = True
            print('Server processing failed during complaince verification due to unexpected error - {}',format(e))
            return_message = 'Server processing failed during complaince verification due to unexpected error - {}',format(e)
   
    
    if verify_activity == 'Coldchain':
        
        if API_flow:
            package = body_dict_payload.get('package')
            batches = body_dict_payload.get('data').get('batch')
        else:
            package = event.get('package')
            batches = event.get('data').get('batch')
        try:
            
            with create_qldb_driver(ledger_name) as driver:
            
                for eachbatch in batches:
    
                    print("Verifying ledger for coldchain on package - {} and batch - {}. Processing ...".format(package, eachbatch))
                            
                    verify_status, coldchain_status = verify_coldchain(driver, ledger_name, package, eachbatch)
                        
                    if verify_status:
                        if coldchain_status == '1':
                            
                            #return_message = return_message + 'Verified ledger coldchain for package: ' + package + ' and batch :' + eachbatch +' - '+ 'Not Broken ;'
                            return_message = ''+ eachbatch + ': Not Broken '
                        else:
                            #return_message = return_message + 'Verified ledger coldchain for package: ' + package + ' and batch :' + eachbatch +' - '+ 'Broken ;'
                            return_message = ''+ eachbatch + ': Broken '
                    else:
                        return_message = return_message + 'Ledger verification failed for package: ' + package + ' and batch :' + eachbatch +'; '
                        
        except Exception as e:
            processingerror = True
            print('Server processing failed during complaince verification due to unexpected error - {}',format(e))
            return_message = 'Server processing failed during complaince verification due to unexpected error - {}',format(e)
   
    return {
        "isBase64Encoded": False,
        "statusCode": 200,
        "body": return_message
    }