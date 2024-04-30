import os
import logging
import json
import asyncio
import aiofiles
from datetime import datetime
import threading
import database_helper as db
import helper

logger = logging.getLogger(__name__)

# Global dictionary to store file paths with their processing status
file_status = {}

class SQLProcessor:
    def __init__(self, incoming_dir, processed_dir, failed_dir, results_dir, pool):
        self.incoming_dir = incoming_dir
        self.processed_dir = processed_dir
        self.failed_dir = failed_dir
        self.results_dir = results_dir
        self.pool = pool

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
            try:
                new_metadata = {'ProcessStatus': 'Success' if is_successful else ('Failed : ' + error), 'TimeProcessed': datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}
                # Remove the file from the dictionary after processing
                self.move_file(file_path, is_successful, new_metadata)
                del file_status[file_path]
            except Exception as e:
                logger.error(f"Error moving or updating metadata for request '{file_name}': {e}")

    def move_file(self, file_path, is_successful, metadata):
        # Determine the destination directory
        dest_dir = self.processed_dir if is_successful else self.failed_dir
        # Move the file and update metadata
        helper.update_metadata_and_move_file(file_path, metadata, dest_dir)
        logger.info(f"Moving {'processed' if is_successful else 'failed'} request '{os.path.basename(file_path)}' to '{dest_dir}' directory")


async def update_file_status(incoming_dir):
    global file_status
    while True:
        # Get the list of files in the incoming directory
        incoming_files = [os.path.join(incoming_dir, f) for f in os.listdir(incoming_dir) if os.path.isfile(os.path.join(incoming_dir, f)) and f.endswith('.json')]
        newFileAdded = False
        # Update the file status dictionary for new files
        for file_path in incoming_files:
            if file_path not in file_status:
                newFileAdded = True
                file_status[file_path] = False
                logger.info("New Files Added")
        if not newFileAdded:
            logger.info("No new files found")
        # Sleep for a while before updating again
        await asyncio.sleep(10)


async def process_files(pool, processor):
    global file_status
    while True:
        # Check for files that haven't been picked
        files_to_process = [file_path for file_path, picked in file_status.items() if not picked]
        if not files_to_process:
            # If there are no files to process, wait for a while
            logger.info("Waiting for files...")
            await asyncio.sleep(10)
            continue
        else:
            tasks = []
            for file_path in files_to_process:
                if pool.number_of_connections < pool.max_connections:
                    # Mark the file as picked
                    file_status[file_path] = True
                    # Process the file asynchronously
                    tasks.append(asyncio.create_task(processor.process_request(file_path)))
            # Wait for all tasks to complete
            await asyncio.gather(*tasks)


async def main():
    config = helper.read_config()
    incoming_dir = config['Directories']['Incoming']
    processed_dir = config['Directories']['Processed']
    failed_dir = config['Directories']['Failed']
    results_dir = config['Directories']['Results']
    username = config['OracleDB']['user']
    password = config['OracleDB']['password']
    dsn = f"{config['OracleDB']['host']}:{config['OracleDB']['port']}/{config['OracleDB']['service_name']}"

    pool = db.DatabasePool(user=username, password=password, dsn=dsn)
    helper.setup_logger(config)
    processor = SQLProcessor(incoming_dir, processed_dir, failed_dir, results_dir, pool)
    # Create directories if they do not exist
    for directory in [incoming_dir, processed_dir, failed_dir]:
        if not os.path.exists(directory):
            os.makedirs(directory)

    # Start updating file status asynchronously
    file_status_task = asyncio.create_task(update_file_status(incoming_dir))

    # Start processing files asynchronously
    file_process_task = asyncio.create_task(process_files(pool, processor))

    await asyncio.gather(file_status_task, file_process_task)

    # Run the event loop indefinitely
    while True:
        try:
            await asyncio.sleep(600)  # Sleep for 10 minutes
        except KeyboardInterrupt:
            break

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt detected. Stopping server...")
    except Exception as e:
        logger.error(f"An error occurred: {e}")