import os
import configparser
import logging
import sys

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
