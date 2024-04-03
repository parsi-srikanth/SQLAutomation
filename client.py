import logging
import helper
from jinja2 import Environment, FileSystemLoader
import os
import time

def generate_sql_request(outgoing_dir, sql_query, output_location, requestor, process_status):
    if not os.path.exists(outgoing_dir):
        os.makedirs(outgoing_dir)
    timestamp = time.strftime('%Y-%m-%d_%H-%M-%S')
    file_name = f'request_{timestamp}.sql'
    file_path = os.path.join(outgoing_dir, file_name)
    with open(file_path, 'w') as file:
        # Write metadata block comment
        file.write("/* Metadata:\n")
        file.write(f"Requestor={requestor}\n")
        file.write(f"TimeRequested={timestamp}\n")
        file.write(f"ProcessStatus={process_status}\n")
        file.write(f"OutputLocation={output_location}\n")
        file.write("*/\n\n")

        # Write SQL query
        file.write(sql_query)

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

if __name__ == "__main__":
    main()
