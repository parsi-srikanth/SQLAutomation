import logging
import helper
from jinja2 import Environment, FileSystemLoader
import os
import time
import json

logger = logging.getLogger(__name__)

def generate_sql_request(outgoing_dir, sql_query, output_location, requestor, process_status):
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
        timestamp = time.strftime('%Y-%m-%d_%H-%M-%S')
        file_name = f'request_{timestamp}.json'
        file_path = os.path.join(outgoing_dir, file_name)
        
        # Replace newline characters with space
        sql_query = sql_query.replace('\n', '')
        
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

def main():
    try:
        config = helper.read_config()
        logger = helper.setup_logger(config)
        
        directory_path = config['SQLFiles']['DirectoryPath']
        connection_string = config['OracleDB']['connection']
        parameters = dict(config.items('parameters'))
        outgoing_dir = config['Directories']['Incoming']
        output_location = config['Directories']['Results']

        env = Environment(loader=FileSystemLoader(directory_path, followlinks=True), trim_blocks=True, lstrip_blocks=True)
        env.globals['tableName'] = parameters['tablename']
        env.globals['ids'] = (1, 2, 3)
        template = env.get_template('HispanicStudents/HispanicStudents.sql')

        sql_query = template.render()

        # Provide metadata parameters
        requestor = "SP"  
        process_status = "Pending"  

        logger.info("Generated SQL query")
        generate_sql_request(outgoing_dir, sql_query, output_location, requestor, process_status)
        logger.info("SQL request generated successfully")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise

if __name__ == "__main__":
    main()
