from time import sleep
from datetime import datetime
from decimal import Decimal

from boto3 import client

from constants import Constants
from pyqldb.driver.qldb_driver import QldbDriver
import json


qldb_client = client('qldb')

LEDGER_CREATION_POLL_PERIOD_SEC = 20
ACTIVE_STATE = "ACTIVE"



def create_ledger(name):
    """
    Create a new ledger with the specified name.

    :type name: str
    :param name: Name for the ledger to be created.

    :rtype: dict
    :return: Result from the request.
    """
    print("Creating ledger named: {}...".format(name))
    result = qldb_client.create_ledger(Name=name, PermissionsMode='STANDARD')
    print('Success. Ledger state: {}.'.format(result.get('State')))
    return result


def wait_for_active(name):
    """
    Wait for the newly created ledger to become active.

    :type name: str
    :param name: The ledger to check on.

    :rtype: dict
    :return: Result from the request.
    """
    print('Waiting for ledger to become active...')
    while True:
        result = qldb_client.describe_ledger(Name=name)
        if result.get('State') == ACTIVE_STATE:
            print('Success. Ledger is active and ready to use.')
            return result
        print('The ledger is still creating. Please wait...')
        sleep(LEDGER_CREATION_POLL_PERIOD_SEC)



def create_qldb_driver(ledger_name, region_name=None, endpoint_url=None, boto3_session=None):
    """
    Create a QLDB driver for executing transactions.

    :type ledger_name: str
    :param ledger_name: The QLDB ledger name.

    :type region_name: str
    :param region_name: See [1].

    :type endpoint_url: str
    :param endpoint_url: See [1].

    :type boto3_session: :py:class:`boto3.session.Session`
    :param boto3_session: The boto3 session to create the client with (see [1]).

    :rtype: :py:class:`pyqldb.driver.qldb_driver.QldbDriver`
    :return: A QLDB driver object.

    [1]: `Boto3 Session.client Reference <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html#boto3.session.Session.client>`.
    """
    qldb_driver = QldbDriver(ledger_name=ledger_name, region_name=region_name, endpoint_url=endpoint_url,
                             boto3_session=boto3_session)
    return qldb_driver



def create_table(driver, table_name):
    """
    Create a table with the specified name.

    :type driver: :py:class:`pyqldb.driver.qldb_driver.QldbDriver`
    :param driver: An instance of the QldbDriver class.

    :type table_name: str
    :param table_name: Name of the table to create.

    :rtype: int
    :return: The number of changes to the database.
    """
    print("Creating the '{}' table...".format(table_name))
    statement = 'CREATE TABLE {}'.format(table_name)
    cursor = driver.execute_lambda(lambda executor: executor.execute_statement(statement))
    print('{} table created successfully.'.format(table_name))
    return len(list(cursor))

def create_index(driver, table_name, index_attribute):
    """
    Create an index for a particular table.

    :type driver: :py:class:`pyqldb.driver.qldb_driver.QldbDriver`
    :param driver: An instance of the QldbDriver class.

    :type table_name: str
    :param table_name: Name of the table to add indexes for.

    :type index_attribute: str
    :param index_attribute: Index to create on a single attribute.

    :rtype: int
    :return: The number of changes to the database.
    """
    print("Creating index on '{}'...".format(index_attribute))
    statement = 'CREATE INDEX on {} ({})'.format(table_name, index_attribute)
    cursor = driver.execute_lambda(lambda executor: executor.execute_statement(statement))
    return len(list(cursor))




def lambda_handler(event, context):
    
    API_flow = False
    processing_error = False
    return_msg = ''
    
    if event.get('body') is None:
        # Lambda test flow
        ledger_name = event.get('ledgername')
        #print(ledger_name)
    else:
        # API Gateway flow
        API_flow = True
        body_dict_payload = json.loads(event.get('body'))
        ledger_name = body_dict_payload.get('ledgername')
        # print(ledger_name)
    
    """
    Create a ledger and wait for it to be active.
    """
    try:
        create_ledger(ledger_name)
        wait_for_active(ledger_name)
    except Exception as e:
        processing_error = True
        print('Unable to create the ledger! - {}'.format(e))
        return_msg = 'Unable to create the ledger! - {}'.format(e)
        #raise e
        
    if not processing_error:
        #proceed ahead
        
        """
        Create required tables.
        """
        print('Creating tables on ledger...')
        try:
            with create_qldb_driver(ledger_name) as driver:
                create_table(driver, Constants.MANUFACTURER_TABLE_NAME)
                create_table(driver, Constants.DISTRIBUTOR_TABLE_NAME)
                create_table(driver, Constants.HOSPITAL_TABLE_NAME)
                create_table(driver, Constants.ITEM_TABLE_NAME)
                create_table(driver, Constants.PHARMACY_TABLE_NAME)
                create_table(driver, Constants.TRANSACTION_TABLE_NAME)
                create_table(driver, Constants.TRANSPORTER_TABLE_NAME)
                create_table(driver, Constants.MEMBER_TABLE_NAME)
                print('Tables created successfully.')
        except Exception as e:
            processing_error = True
            print('Errors creating tables - {}',format(e))
            return_msg = 'Errors creating tables - {}',format(e)
            
    
    if not processing_error:
        #proceed ahead
    
        """
        Create indexes on tables in a particular ledger.
        """
        print('Creating indexes on all tables...')
        try:
            with create_qldb_driver(ledger_name) as driver:
                create_index(driver, Constants.DISTRIBUTOR_TABLE_NAME, Constants.DISTRIBUTOR_LICENSE_NUM_INDEX_NAME)
                create_index(driver, Constants.DISTRIBUTOR_TABLE_NAME, Constants.DISTRIBUTOR_REGISTRATION_NUM_INDEX_NAME)
                create_index(driver, Constants.HOSPITAL_TABLE_NAME, Constants.HOSPITAL_LICENSE_NUM_INDEX_NAME)
                create_index(driver, Constants.HOSPITAL_TABLE_NAME, Constants.HOSPITAL_REGISTRATION_NUM_INDEX_NAME)
                create_index(driver, Constants.ITEM_TABLE_NAME, Constants.ITEM_ID_INDEX_NAME)
                create_index(driver, Constants.ITEM_TABLE_NAME, Constants.ITEM_MFG_BATCH_NUMBER_INDEX_NAME)
                create_index(driver, Constants.ITEM_TABLE_NAME, Constants.ITEM_PKG_LABEL_INDEX_NAME)
                create_index(driver, Constants.ITEM_TABLE_NAME, Constants.ITEM_PKG_OWNER_INDEX_NAME)
                create_index(driver, Constants.MANUFACTURER_TABLE_NAME, Constants.MANUFACTURER_LICENSE_NUM_INDEX_NAME)
                create_index(driver, Constants.MANUFACTURER_TABLE_NAME, Constants.MANUFACTURER_REGISTRATION_NUM_INDEX_NAME)
                create_index(driver, Constants.PHARMACY_TABLE_NAME, Constants.PHARMACY_LICENSE_NUM_INDEX_NAME)
                create_index(driver, Constants.PHARMACY_TABLE_NAME, Constants.PHARMACY_REGISTRATION_NUM_INDEX_NAME)
                create_index(driver, Constants.TRANSPORTER_TABLE_NAME, Constants.TRANSPORTER_LICENSE_NUM_INDEX_NAME)
                create_index(driver, Constants.TRANSPORTER_TABLE_NAME, Constants.TRANSPORTER_REGISTRATION_NUM_INDEX_NAME)
                create_index(driver, Constants.TRANSACTION_TABLE_NAME, Constants.TRANSACTION_LABEL_INDEX_NAME)
                create_index(driver, Constants.TRANSACTION_TABLE_NAME, Constants.TRANSACTION_STATE_INDEX_NAME)
                create_index(driver, Constants.TRANSACTION_TABLE_NAME, Constants.TRANSACTION_OWNER_INDEX_NAME)
                create_index(driver, Constants.TRANSACTION_TABLE_NAME, Constants.TRANSACTION_DATA_INDEX_NAME)
                create_index(driver, Constants.TRANSACTION_TABLE_NAME, Constants.TRANSACTION_VERSION_INDEX_NAME)
                create_index(driver, Constants.MEMBER_TABLE_NAME, Constants.MEMBER_EMAIL_INDEX_NAME)
                create_index(driver, Constants.MEMBER_TABLE_NAME, Constants.MEMBER_DOCID_INDEX_NAME)
                create_index(driver, Constants.MEMBER_TABLE_NAME, Constants.MEMBER_USERNAME_INDEX_NAME)
                create_index(driver, Constants.MEMBER_TABLE_NAME, Constants.MEMBER_USERTYPE_INDEX_NAME)
                
                print('Indexes created successfully.')
        except Exception as e:
            processing_error = True
            print('Unable to create indexes- {}',format(e))
            return_msg = 'Unable to create indexes- {}',format(e)
               
    
    if not processing_error:
        return_msg = "Ledger Initialization completed successfully"
        
        
    response = {
        "isBase64Encoded": "false",
        "statusCode": 200,
        "body": return_msg
    }
    
    return response