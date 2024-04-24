import os
import logging
import aiofiles
import pandas as pd
import csv
import asyncio
import cx_Oracle
import json

logger = logging.getLogger(__name__)

#TODO : add instrcutions to install Oracle Instant Client and cx_Oracle in the README.md file

# Uncomment below line if you are using Oracle Instant Client. Make sure to provide the correct path to the instant client directory.
#cx_Oracle.init_oracle_client(lib_dir=r"C:\oracle\instantclient_19_9")

class DatabasePool:
    def __init__(self, user, password, dsn, min_connections=1, max_connections=3, increment=1, timeout=600, encoding="UTF-8"):
        self.user = user
        self.password = password
        self.dsn = dsn
        self.min_connections = min_connections
        self.max_connections = max_connections
        self.increment = increment
        self.encoding = encoding
        self.timeout = timeout
        self.pool = None

    async def create_pool(self):
        self.pool = cx_Oracle.SessionPool(
            user=self.user,
            password=self.password,
            dsn=self.dsn,
            min=self.min_connections,
            max=self.max_connections,
            increment=self.increment,
            encoding=self.encoding,
            timeout=self.timeout,
            getmode=cx_Oracle.SPOOL_ATTRVAL_WAIT  # Wait for a connection if no connections are available
        )

    async def get_connection(self):
        if self.pool is None:
            await self.create_pool()
        return self.pool.acquire()

    async def release_connection(self, connection):
        self.pool.release(connection)

    async def close_pool(self):
        if self.pool:
            self.pool.close()
            self.pool = None


async def execute_query_and_store_output(pool, sql_query, output_file_location, output_file_name, output_file_extension):
    con = await pool.get_connection()  # Await the coroutine function
    logger.info("Connected to database")

    try:
        cursor = con.cursor()
        cursor.arraysize = 1000
        logger.info("Executing query")
        cursor.execute(sql_query)
        logger.info("Query executed successfully")
        
        if not os.path.exists(output_file_location):
            os.makedirs(output_file_location, exist_ok=True)
        output_file = os.path.join(output_file_location, output_file_name + output_file_extension)
        
        if output_file_extension == ".xlsx":
            writer = pd.ExcelWriter(output_file, engine='xlsxwriter')
        elif output_file_extension == ".csv":
            writer = csv.writer(open(output_file, 'w', newline='', encoding='utf-8'))
        else:
            raise ValueError("Unsupported output file format. Supported formats are Excel (.xlsx) and CSV (.csv)")

        columns = [col[0] for col in cursor.description]
        writer.writerow(columns)
        logger.info("Writing data to file")

        # Fetch rows in batches and write them to the file
        while True:
            rows = cursor.fetchmany(1000)  # Fetch 1000 rows at a time
            if not rows:
                break
            await asyncio.get_event_loop().run_in_executor(None, writer.writerows, rows)

        logger.info(f"Data stored successfully to {output_file}")

    except Exception as e:
        logger.error(f"Error: {e}")
        raise  # Re-raise the exception to propagate it further

    finally:
        await pool.release_connection(con)  # Await the coroutine function
        logger.info("Database connection released")
