import os
import cx_Oracle
from datetime import datetime
import database_helper as db

incoming_dir = 'incoming_requests/'
processed_dir = 'processed_requests/'

def watch_directory():
    while True:
        files = os.listdir(incoming_dir)
        for file_name in files:
            if file_name.endswith('.sql'):
                process_request(file_name)
                os.remove(os.path.join(incoming_dir, file_name))

def process_request(file_name):
    with open(os.path.join(incoming_dir, file_name), 'r') as file:
        lines = file.readlines()
        metadata = lines[0].strip()
        sql_query = ''.join(lines[1:])
    file_name = file_name.split('.')[0]
    output_location = metadata.split(':')[1].strip()
    connection_string = config['OracleDB']['connection']
    db.execute_query_and_store_output(connection_string, sql_query, output_location, file_name, ".csv")
    
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    processed_file_name = f"{timestamp}_{file_name}"
    os.rename(os.path.join(incoming_dir, file_name), os.path.join(processed_dir, processed_file_name + ".sql"))

if __name__ == "__main__":
    config = helper.read_config()
    logging = helper.create_logger()
    logger = logging.getLogger(__name__)

    if not os.path.exists(processed_dir):
        os.makedirs(processed_dir)
    
    watch_directory()
