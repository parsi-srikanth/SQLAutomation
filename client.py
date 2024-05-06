import logging
import helper
from jinja2 import Environment, FileSystemLoader
import os
import time
import json
import sys

logger = logging.getLogger(__name__)

def generate_sql_request(outgoing_dir, sql_query, output_location, requestor, process_status, file_name):
    """Generate an SQL request JSON file.

    Args:
        outgoing_dir (str): The directory to save the JSON file.
        sql_query (str): The SQL query to be written in the file.
        output_location (str): The location where the output will be stored.
        requestor (str): The entity making the request.
        process_status (str): The status of the process.

    Returns:
        str: The path of the generated JSON file.
    """
    try:
        if not os.path.exists(outgoing_dir):
            os.makedirs(outgoing_dir)
        timestamp = time.strftime('%m-%d-%Y %I-%M-%S %p')
        file_name = f'{file_name}_{timestamp}.json'
        file_path = os.path.join(outgoing_dir, file_name)
        
        # Construct data dictionary
        data = {
            "Metadata": {
                "Requestor": requestor,
                "TimeRequested": timestamp,
                "ProcessStatus": process_status,
                "OutputLocation": output_location
            },
            "SQLQuery": sql_query
        }
        
        # Write data to JSON file
        with open(file_path, 'w') as file:
            json.dump(data, file, indent=4)

        return file_path
    except Exception as e:
        logger.error(f"Error generating SQL request JSON file: {e}")
        raise

def main(passed_sql_file):
    try:
        # Set up config and logger
        config = helper.read_config()
        logger = helper.setup_logger(config)
        
        # Set up jinja environment based provided file, or default if no file passed.
        template_directorypath = config['SQLFiles']['template_directorypath']

        if passed_sql_file:
            logger.info(f"SQL file: {passed_sql_file} is being used.")
            file_path = os.path.dirname(passed_sql_file)
            template = os.path.basename(passed_sql_file)
            file_name = template.replace('.sql', '')
        else:
            logger.info("No file specified. Using the default SQL file.")
            defaut_template = config['SQLFiles']['default_template']
            file_path = os.path.dirname(os.path.realpath(defaut_template))
            file_name = os.path.basename(defaut_template).replace('.sql', '')
            template = defaut_template
        
        env = Environment(loader=FileSystemLoader([file_path, template_directorypath], followlinks=True), trim_blocks=True, lstrip_blocks=True)
        
        # Load in optional paramaters
        parameters = dict(config.items('parameters'))
        env.globals['tableName'] = parameters['tablename']
        env.globals['ids'] = (1, 2, 3)

        sql_query = env.get_template(template).render()
        logger.info("Generated SQL query")

        outgoing_dir = config['Directories']['Incoming']
        output_location = config['client']['output_location']

        if not os.path.exists(outgoing_dir):
            os.makedirs(outgoing_dir)

        # Provide metadata parameters
        requestor = config['client']['requestor']
        process_status = config['client']['process_status']
        
        generate_sql_request(outgoing_dir, sql_query, output_location, requestor, process_status, file_name)
        logger.info("SQL request generated successfully")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise

if __name__ == "__main__":
    if len(sys.argv) < 2:
        main(None)
    else:
        main(sys.argv[1])
