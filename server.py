import os
import logging
import json
import asyncio
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import aiofiles
import aiofiles.os
import helper
import database_helper as db

logger = logging.getLogger(__name__)

class SQLRequestHandler(FileSystemEventHandler):
    def __init__(self, incoming_dir, processed_dir, failed_dir, results_dir, pool):
        self.incoming_dir = incoming_dir
        self.processed_dir = processed_dir
        self.failed_dir = failed_dir
        self.results_dir = results_dir
        self.pool = pool

    async def on_created(self, event):
        if not event.is_directory and event.src_path.endswith('.json'):
            try:
                await self.process_request(event.src_path)
            except Exception as e:
                logger.error(f"Error processing request '{event.src_path}': {e}")

    async def process_request(self, file_path):
        is_successful = False
        try:
            logger.info(f"Processing request '{file_path}'")
            file_name = os.path.basename(file_path).replace('.json', '')
            async with aiofiles.open(file_path, 'r') as json_file:
                data = json.loads(await json_file.read())
                metadata = data.get('Metadata', {})
                sql_query = data.get('SQLQuery', '')
            output_location = metadata.get('OutputLocation', self.results_dir)
            await db.execute_query_and_store_output(self.pool, sql_query, output_location, file_name, ".csv")
            is_successful = True
        except Exception as e:
            error = str(e)
            logger.error(f"Error processing request '{file_name}': {error}")
            raise  # Re-raise the exception to propagate it further
        finally:
            new_metadata = {'ProcessStatus': 'Success' if is_successful else ('Failed : ' + error), 'TimeProcessed': datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}
            try:
                helper.update_metadata_and_move_file(file_path, new_metadata, self.processed_dir if is_successful else self.failed_dir)
                logger.info(f"Moving {'processed' if is_successful else 'failed'} request '{file_name}.json' to '{self.processed_dir if is_successful else self.failed_dir}' directory")
            except Exception as e:
                logger.error(f"Error moving or updating metadata for request '{file_name}': {e}")
                helper.update_metadata_and_move_file(file_path, {'ProcessStatus': 'Failed', 'TimeProcessed': datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}, self.failed_dir)
                raise  # Re-raise the exception to propagate it further

async def process_existing_files(incoming_dir, processed_dir, failed_dir, results_dir, pool):
    existing_files = [os.path.join(incoming_dir, f) for f in os.listdir(incoming_dir) if os.path.isfile(os.path.join(incoming_dir, f))]
    for file_path in existing_files:
        try:
            event_handler = SQLRequestHandler(incoming_dir, processed_dir, failed_dir, results_dir, pool)
            await event_handler.process_request(file_path)
        except Exception as e:
            logger.error(f"Error processing existing file '{file_path}': {e}")

async def main():
    config = helper.read_config()
    incoming_dir = config['Directories']['Incoming']
    processed_dir = config['Directories']['Processed']
    failed_dir = config['Directories']['Failed']
    results_dir = config['Directories']['Results']
    username = config['OracleDB']['user']
    password = config['OracleDB']['password']
    dsn = config['OracleDB']['DSN']

    pool = db.DatabasePool(user=username, password=password, dsn=dsn)
    helper.setup_logger(config)

    # Create directories if they do not exist
    for directory in [processed_dir, failed_dir]:
        if not os.path.exists(directory):
            os.makedirs(directory)

    await process_existing_files(incoming_dir, processed_dir, failed_dir, results_dir, pool)

    # Start watchdog observer
    observer = Observer()
    sql_request_handler = SQLRequestHandler(incoming_dir, processed_dir, failed_dir, results_dir, pool)
    observer.schedule(sql_request_handler, path=incoming_dir, recursive=False)
    observer.start()

    try:
        logger.info(f"Watching directory '{incoming_dir}' for SQL request files...")
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt detected. Stopping server...")
        observer.stop()
        logger.info("Observer stopped.")
    except Exception as e:
        logger.error(f"Error in server: {e}")
        observer.stop()
        logger.info("Observer stopped due to an error.")
    finally:
        await pool.close_pool()
        logger.info("Database connection pool closed.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt detected. Stopping server...")
    except Exception as e:
        logger.error(f"An error occurred: {e}")