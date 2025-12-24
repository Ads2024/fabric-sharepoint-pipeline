import pyodbc
import os
import logging
import pyodbc
from typing import List, Dict,Optional
from azure.identity import ClientSecretCredential

logger = logging.getLogger(__name__)


def get_fabric_connection(tennat_id: str, client_id: str, client_secret: str, sql_endpoint: str, database: str):
    try:
        logger.info(f"Authenticating with Azure AD for SQL endpoint: {sql_endpoint}")
        credential = ClientSecretCredential(
            tenant_id=tennat_id,
            client_id=client_id,
            client_secret=client_secret
        )
        token = credential.get_token("https://database.windows.net/.default")

        try:
            drivers = [driver for driver in pyodbc.drivers()]
            if "ODBC Driver 18 for SQL Server" in drivers:
                driver_name = "ODBC Driver 18 for SQL Server"
            elif "ODBC Driver 17 for SQL Server" in drivers:
                driver_name = "ODBC Driver 17 for SQL Server"
            else:
                raise Exception("ODBC Driver 18 or 17 for SQL Server not found")
        except Exception as e:
            logger.warning(f"Could not detect ODBC Driver, defaulting to Driver 18:")
            driver_name = "ODBC Driver 18 for SQL Server"
        logger.info(f"Using ODBC Driver: {driver_name}")

        connection_string = (
            f"Driver={driver_name};"
            f"Server=tcp:{sql_endpoint},1433;"
            f"Database={database};"
            f"Authentication=ActiveDirectoryServicePrincipal;"
            f"Uid={client_id};"
            f"Pwd={client_secret};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
            f"Connection Timeout=30;"
        )

        conn = pyodbc.connect(connection_string)
        logger.info("Successfully connected to SQL endpoint")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to SQL endpoint: {str(e)}")
        raise


def execute_query(connection: pyodbc.Connection, query:str):
    try:
        logger.info(f"Executing query: {query}")
        cursor = connection.cursor()
        cursor.execute(query)
        
        columns = [column [0] for column in cursor.description]

        results = []
        for row in cursor.fetchall():
            row_dict = dict(zip(columns, row))
            results.append(row_dict)
        
        cursor.close()
        logger.info(f"Query executed successfully. Retrieved {len(results)} rows")

        return results
    except Exception as e:
        logger.error(f"Failed to execute query: {str(e)}")
        raise



def get_areas_list(tenant_id: str, client_id: str, client_secret: str, sql_endpoint: str, database: str, query: str):
    conn = None
    try:
        conn = get_fabric_connection(tenant_id, client_id, client_secret, sql_endpoint, database)
        results = execute_query(conn, query)
        
        areas = []
        if results:
            first_key = list(results[0].keys())[0]
            areas = [row[first_key] for row in results if row.get(first_key)]
            
        logger.info(f"Retrieved {len(areas)} areas")
        return areas
    
    except Exception as e:
        logger.error(f"Failed to get areas list: {e}")
        raise

    finally:
        if conn:
            conn.close()

def get_specialised_carers_list(tenant_id: str, client_id: str, client_secret: str, sql_endpoint: str, database: str, query: str):
    # TODO: This function name is specific to the current use case, but it should be more generic
    # will use this function for now
    return get_salesforce_data(tenant_id, client_id, client_secret, sql_endpoint, database, query)


def get_salesforce_data(tenant_id: str, client_id: str, client_secret: str, sql_endpoint: str, database: str, query: str):
    conn = None
    try:
        conn = get_fabric_connection(tenant_id, client_id, client_secret, sql_endpoint, database)
        results = execute_query(conn, query)
        logger.info(f"Retrieved {len(results)} records")
        return results
    
    except Exception as e:
        logger.error(f"Error querying data: {e}")
        return []
    
    finally:
        if conn:
            conn.close()