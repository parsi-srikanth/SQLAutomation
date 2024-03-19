import os
import cx_Oracle
import pandas as pd
import csv
import logging

logger = logging.getLogger(__name__)

def execute_query_and_store_output(connection_string, sql_query, output_file_location, output_file_name, output_file_extension):
    try:
        con = cx_Oracle.connect(connection_string)
        logger.info("Connected to database")

        with con.cursor() as cursor:
            cursor.arraysize = 1000
            logger.info("Executing query")
            cursor.execute(sql_query)
            logger.info("Query executed successfully")
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
            con.close()
            logger.info("Database connection closed")
