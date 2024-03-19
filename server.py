import os
from datetime import datetime
import helper
import database_helper as db
import shutil

def watch_directory(incoming_dir, processed_dir):
    while True:
        files = os.listdir(incoming_dir)
        if len(files) == 0:
            break
        for file_name in files:
            if file_name.endswith('.sql'):
                process_request(file_name, incoming_dir, processed_dir)

def process_request(file_name, incoming_dir, processed_dir):
    try:
        with open(helper.get_file_path(file_name, incoming_dir), 'r') as file:
            lines = file.readlines()
            metadata = lines[0].strip()
            sql_query = ''.join(lines[1:])
        file_name = file_name.split('.')[0]
        output_location = metadata.split(':')[1].strip()
        config = helper.read_config()
        connection_string = config['OracleDB']['connection']
        db.execute_query_and_store_output(connection_string, sql_query, output_location, file_name, ".csv")
        logger.info(f"Request '{file_name}' processed successfully")
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        processed_file_name = f"{timestamp}_{file_name}"
        logger.info(f"Renaming file '{file_name}.sql' to '{processed_file_name}.sql' and moving from '{incoming_dir}' to '{processed_dir}' directory ")
        shutil.move(
            helper.get_file_path(file_name + ".sql", incoming_dir),
            helper.get_file_path(processed_file_name + ".sql", processed_dir)
        )
    except Exception as e:
        logger.error(f"Error processing request '{file_name}': {e}")

if __name__ == "__main__":
    config = helper.read_config()
    incoming_dir = config['Directories']['Incoming']
    processed_dir = config['Directories']['Processed']
    if not os.path.exists(processed_dir):
        os.makedirs(processed_dir)
    logger = helper.setup_logger(config)
    watch_directory(incoming_dir, processed_dir)
