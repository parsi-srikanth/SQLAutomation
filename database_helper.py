import os
import cx_Oracle
import pandas as pd
import csv
import logging

logger = logging.getLogger(__name__)

import cx_Oracle

class DatabasePool:
    def __init__(self, user, password, dsn, min_connections=2, max_connections=5, increment=1, encoding="UTF-8"):
        self.user = user
        self.password = password
        self.dsn = dsn
        self.min_connections = min_connections
        self.max_connections = max_connections
        self.increment = increment
        self.encoding = encoding
        self.pool = None

    def create_pool(self):
        self.pool = cx_Oracle.SessionPool(
            user=self.user,
            password=self.password,
            dsn=self.dsn,
            min=self.min_connections,
            max=self.max_connections,
            increment=self.increment,
            encoding=self.encoding
        )

    def get_connection(self):
        if self.pool is None:
            self.create_pool()
        return self.pool.acquire()

    def release_connection(self, connection):
        self.pool.release(connection)

    def close_pool(self):
        if self.pool:
            self.pool.close()
            self.pool = None


def execute_query_and_store_output(pool, sql_query, output_file_location, output_file_name, output_file_extension):
    con = None
    try:
        con = pool.get_connection()
        logger.info("Connected to database")

        with con.cursor() as cursor:
            cursor.arraysize = 1000
            logger.info("Executing query")
            cursor.execute(sql_query)
            logger.info("Query executed successfully")
            
            # Save data to file, etc.
            if not os.path.exists(output_file_location):
                os.makedirs(output_file_location, exist_ok=True)
            output_file = os.path.join(output_file_location, output_file_name + output_file_extension)
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                if output_file_extension == ".xlsx":
                    writer = pd.ExcelWriter(f, engine='xlsxwriter')
                elif output_file_extension == ".csv":
                    writer = csv.writer(f)
                else:
                    logger.error("Unsupported output file format. Supported formats are Excel (.xlsx) and CSV (.csv)")
                    return

                columns = [col[0] for col in cursor.description]
                writer.writerow(columns)

                while True:
                    rows = cursor.fetchmany()
                    if not rows:
                        break
                    writer.writerows(rows)

            logger.info(f"Data stored successfully to {output_file}")

    except Exception as e:
        logger.error(f"Error: {e}")

    finally:
        if con:
            pool.release_connection(con)
            logger.info("Database connection released")

