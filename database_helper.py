import os
import logging
import configparser
import cx_Oracle
import pandas as pd

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
                            logger.info("execute query")
                            columns = [col[0] for col in cursor.description]
                            data = cursor.fetchall()
                            logger.info("fetched results")
                            result = pd.DataFrame(data, columns=columns)
                            result.to_csv(os.path.join(folder_path, f"{os.path.splitext(file)[0]}.csv"), index=False)
                            logger.info(f"csv created in {folder_path}")
                            cursor.close()
                            con.close()
                except Exception as e:
                    print(e)
                    logger.error(f"Error: {e}") 