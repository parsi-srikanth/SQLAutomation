import os
import logging
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import helper
import database_helper as db

class SQLRequestHandler(FileSystemEventHandler):
    def __init__(self, incoming_dir, processed_dir, failed_dir, results_dir, connection_string):
        self.incoming_dir = incoming_dir
        self.processed_dir = processed_dir
        self.failed_dir = failed_dir
        self.results_dir = results_dir
        self.connection_string = connection_string

    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith('.sql'):
            self.process_request(event.src_path)

    def process_request(self, file_path):
        try:
            logger.info(f"Processing request '{file_path}'")
            isSuccessful = False
            metadata, sql_query = helper.extract_metadata_and_sql_from_file(file_path)
            
            file_name = file_path.split('/')[-1].replace('.sql', '')
            output_location = metadata.get('OutputLocation', self.results_dir)
            
            db.execute_query_and_store_output(self.connection_string, sql_query, output_location, file_name, ".csv")
            isSuccessful = True
        except Exception as e:
            logger.error(f"Error processing request '{file_name}': {e}")
        finally:
            new_metadata = {'ProcessStatus': 'Success' if isSuccessful else 'Failed', 'TimeProcessed': datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}
            helper.update_metadata_and_move_file(file_path, new_metadata, self.processed_dir if isSuccessful else self.failed_dir)
            logger.info(f"Moving {'processed' if isSuccessful else 'failed'} request '{file_name}.sql' to '{self.processed_dir if isSuccessful else self.failed_dir}' directory")
            
if __name__ == "__main__":
    config = helper.read_config()
    incoming_dir = config['Directories']['Incoming']
    processed_dir = config['Directories']['Processed']
    failed_dir = config['Directories']['Failed']
    results_dir = config['Directories']['Results']
    connection_string = config['OracleDB']['connection']
    if not os.path.exists(processed_dir):
        os.makedirs(processed_dir)
    if not os.path.exists(failed_dir):
        os.makedirs(failed_dir)
    logger = helper.setup_logger(config)

    event_handler = SQLRequestHandler(incoming_dir, processed_dir, failed_dir, results_dir, connection_string)
    observer = Observer()
    observer.schedule(event_handler, path=incoming_dir, recursive=False)
    observer.start()

    try:
        logger.info(f"Watching directory '{incoming_dir}' for SQL request files...")
        observer.join()
    except KeyboardInterrupt:
        observer.stop()
        logger.info("Server stopped")
