import os
import configparser
import logging
import sys
import shutil
import json

logger = logging.getLogger(__name__)

def read_config(config_file='configurations.ini'):
    try:
        config = configparser.ConfigParser()
        config.read(config_file)
        return config
    except Exception as e:
        logger.error(f"Error reading configuration file '{config_file}': {e}")
        raise

def setup_logger(config):
    try:
        log_file_path = config['Logger']['LogFilePath']
        log_file_name = config['Logger']['LogFileName']
        log_level = config['Logger']['LogLevel']
        log_file = os.path.join(log_file_path, log_file_name)

        logging.basicConfig(
            format='%(asctime)s, %(name)s %(levelname)s %(message)s',
            level=log_level,
            handlers=[
                logging.FileHandler(log_file, mode='w'),
                logging.StreamHandler(sys.stdout)
            ]
        )

        return logging.getLogger(__name__)
    except Exception as e:
        logger.error(f"Error setting up logger: {e}")
        raise

def get_file_path(file_name, directory):
    try:
        return os.path.join(directory, file_name)
    except Exception as e:
        logger.error(f"Error getting file path: {e}")
        raise


def extract_metadata_and_sql_from_file(file_path):
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
            metadata = data.get('Metadata', {})
            sql_query = data.get('SQLQuery', '')
        return metadata, sql_query.strip()
    except Exception as e:
        logger.error(f"Error extracting metadata and SQL from file '{file_path}': {e}")
        raise

def update_metadata_and_move_file(json_file_path, new_metadata, destination_folder):
    try:
        # Read the contents of the file
        with open(json_file_path, 'r') as file:
            data = json.load(file)

        # Merge the existing metadata with the new metadata
        merged_metadata = {**data['Metadata'], **new_metadata}
        data['Metadata'] = merged_metadata

        # Write the updated content back to the file
        with open(json_file_path, 'w') as file:
            json.dump(data, file, indent=4)

        # Move the file to the specified location
        file_name = os.path.basename(json_file_path)
        shutil.move(json_file_path, os.path.join(destination_folder, file_name))

        if 'Failed' in data['Metadata']['ProcessStatus']:
            #copy output file to failed directory
            output_file = data['Metadata']['OutputLocation']
            output_file_name = file_name.replace('.json', '.csv')
            if os.path.exists(os.path.join(output_file, output_file_name)):
                shutil.copy(os.path.join(output_file, output_file_name), os.path.join(destination_folder, output_file_name))
                logger.info(f"Output file '{output_file_name}' copied to '{destination_folder}' directory")
            else:
                logger.error(f"Output file '{output_file_name}' not found in '{output_file}' directory")

        return True  # Success
    except Exception as e:
        logger.error(f"Error updating metadata and moving file '{json_file_path}': {e}")
        raise
