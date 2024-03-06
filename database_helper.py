import os
import logging
import configparser
import cx_Oracle
import pandas as pd
from time import sleep

logger = logging.getLogger(__name__)

def connect_to_database(connection_string):
    try:
        con = cx_Oracle.connect(connection_string)
        logger.info("Connected to database")
        return con
    except Exception as e:
        logger.error(f"Error: {e}")
        return None

def execute_sql_queries(directory, parameters, connection_string):
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".sql"):
                file_path = os.path.join(root, file)
                folder_path = os.path.dirname(file_path)
                try:
                    with open(file_path, 'r') as f:
                        sql_query = f.read()
                        for key, value in parameters.items():
                            sql_query = sql_query.replace(key, value)
                        logger.info(f"SQL query after parameter replacement: {sql_query}")
                        con = connect_to_database(connection_string)
                        if con:
                            cursor = con.cursor()
                            cursor.execute(sql_query)
                            logger.info("Executing query")
                            columns = [col[0] for col in cursor.description]
                            # while True:
                            #     if cursor.fetchone() is None:
                            #         break
                            #     logger.info("Query is still running...")
                            #     sleep(15)  # Log every 15 seconds
                            data = fetch_large_data(cursor)
                            logger.info("Query execution completed")
                            cursor.close()
                            con.close()
                            if data:
                                result = pd.DataFrame(data, columns=columns)
                                excel_path = os.path.join(folder_path, f"{os.path.splitext(file)[0]}.xlsx")
                                try:
                                    result.to_excel(excel_path, index=False)
                                    logger.info(f"Excel created at {excel_path}")
                                except Exception as e:
                                    logger.error(f"Error creating Excel file: {e}")
                                    # If Excel conversion fails, save as CSV
                                    csv_path = os.path.join(folder_path, f"{os.path.splitext(file)[0]}.csv")
                                    result.to_csv(csv_path, index=False)
                                    logger.info(f"CSV created at {csv_path}. Excel conversion failed.")
                            else:
                                logger.warning("No data returned from query.")
                except Exception as e:
                    logger.error(f"Error: {e}")

def fetch_large_data(cursor, chunksize=1000):
    data = []
    while True:
        chunk = cursor.fetchmany(chunksize)
        if not chunk:
            break
        data.extend(chunk)
        logger.info("Fetching from chunk...")
    return data
