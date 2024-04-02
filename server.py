import os
import shutil
import logging
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import helper
import database_helper as db

class SQLRequestHandler(FileSystemEventHandler):
    def __init__(self, incoming_dir, processed_dir, failed_dir):
        self.incoming_dir = incoming_dir
        self.processed_dir = processed_dir
        self.failed_dir = failed_dir

    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith('.sql'):
            self.process_request(event.src_path)

    def process_request(self, file_path):
        try:
            file_name = os.path.basename(file_path)
            with open(file_path, 'r') as file:
                lines = file.readlines()
                metadata = lines[0].strip()
                sql_query = ''.join(lines[1:])
            file_name = file_name.split('.')[0]
            output_location = metadata.split('=')[1].strip()
            config = helper.read_config()
            connection_string = config['OracleDB']['connection']
            db.execute_query_and_store_output(connection_string, sql_query, output_location, file_name, ".csv")
            logger.info(f"Request '{file_name}' processed successfully")
            timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
            processed_file_name = f"{timestamp}_{file_name}.sql"
            logger.info(f"Renaming file '{file_name}.sql' to '{processed_file_name}' and moving from '{self.incoming_dir}' to '{self.processed_dir}' directory ")
            shutil.move(file_path, os.path.join(self.processed_dir, processed_file_name))
        except Exception as e:
            logger.error(f"Error processing request '{file_name}': {e}")
            logger.info(f"Moving failed request '{file_name}.sql' to '{self.failed_dir}' directory")
            shutil.move(file_path, os.path.join(self.failed_dir, f"{file_name}_failed.sql"))

if __name__ == "__main__":
    config = helper.read_config()
    incoming_dir = config['Directories']['Incoming']
    processed_dir = config['Directories']['Processed']
    failed_dir = config['Directories']['Failed']
    if not os.path.exists(processed_dir):
        os.makedirs(processed_dir)
    if not os.path.exists(failed_dir):
        os.makedirs(failed_dir)
    logger = helper.setup_logger(config)

    event_handler = SQLRequestHandler(incoming_dir, processed_dir, failed_dir)
    observer = Observer()
    observer.schedule(event_handler, path=incoming_dir, recursive=False)
    observer.start()

    try:
        logger.info(f"Watching directory '{incoming_dir}' for SQL request files...")
        observer.join()
    except KeyboardInterrupt:
        observer.stop()
        logger.info("Server stopped")
