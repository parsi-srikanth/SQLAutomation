import os
import configparser
import logging
import sys
import shutil

def read_config(config_file='configurations.ini'):
    config = configparser.ConfigParser()
    config.read(config_file)
    return config

def setup_logger(config):
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

def get_file_path(file_name, directory):
    return os.path.join(directory, file_name)

def extract_metadata_and_sql_from_file(sql_file_path):
    metadata = {}
    sql_query = ''
    with open(sql_file_path, 'r') as sql_file:
        reading_metadata = False
        for line in sql_file:
            if line.strip().startswith('/* Metadata:'):
                # Start reading metadata
                reading_metadata = True
                continue
            if reading_metadata:
                if line.strip() == '*/':
                    # End of metadata block
                    reading_metadata = False
                else:
                    key, value = line.strip().split('=')
                    metadata[key.strip()] = value.strip()
            else:
                # Collect SQL query
                sql_query += line
    return metadata, sql_query.strip()

def update_metadata_and_move_file(sql_file_path, new_metadata, destination_folder):
    try:
        # Read the contents of the file
        with open(sql_file_path, 'r') as file:
            file_content = file.read()

        # Update metadata in the file content
        file_content = update_metadata_in_content(file_content, new_metadata)

        # Write the updated content back to the file
        with open(sql_file_path, 'w') as file:
            file.write(file_content)

        # Move the file to the specified location
        file_name = os.path.basename(sql_file_path)
        shutil.move(sql_file_path, os.path.join(destination_folder, file_name))

        return True  # Success
    except Exception as e:
        logger.error(f"Error updating metadata and moving file: {e}")
        return False  # Error occurred

def update_metadata_in_content(file_content, new_metadata):
    # Find the metadata block and update metadata
    metadata_index = file_content.find('/* Metadata:')
    if metadata_index != -1:
        end_metadata_index = file_content.find('*/', metadata_index)
        metadata_block = file_content[metadata_index:end_metadata_index + 2]

        # Extract existing metadata
        existing_metadata = {}
        for line in metadata_block.split('\n')[1:-1]:
            key, value = line.strip().split('=')
            existing_metadata[key.strip()] = value.strip()

        # Merge existing metadata with new metadata
        updated_metadata = {**existing_metadata, **new_metadata}

        # Build updated metadata block
        updated_metadata_block = '/* Metadata:\n'
        for key, value in updated_metadata.items():
            updated_metadata_block += f"{key}={value}\n"
        updated_metadata_block += '*/\n'

        # Replace existing metadata block with updated metadata block
        file_content = file_content.replace(metadata_block, updated_metadata_block)

    return file_content
