import logging
import helper
from jinja2 import Environment, FileSystemLoader
import os
import time

def generate_sql_request(outgoing_dir, sql_query, output_location):
    if not os.path.exists(outgoing_dir):
        os.makedirs(outgoing_dir)
    timestamp = time.strftime('%Y-%m-%d_%H-%M-%S')
    file_name = f'request_{timestamp}.sql'
    file_path = os.path.join(outgoing_dir, file_name)
    with open(file_path, 'w') as file:
        file.write(f"-- Metadata - Output Location: {output_location}\n")
        file.write(sql_query)

def main():
    try:
        config = helper.read_config()
        logger = helper.setup_logger(config)
        
        directory_path = config['SQLFiles']['DirectoryPath']
        connection_string = config['OracleDB']['connection']
        parameters = dict(config.items('parameters'))
        outgoing_dir = config['Directories']['Incoming']
        output_location = config['Directories']['results']

        env = Environment(loader=FileSystemLoader(directory_path, followlinks=True), trim_blocks=True, lstrip_blocks=True)
        env.globals['tableName'] = parameters['tablename']
        env.globals['ids'] = (1, 2, 3)
        template = env.get_template('HispanicStudents/HispanicStudents.sql')

        sql_query = template.render()

        logger.info("Generated SQL query")
        generate_sql_request(outgoing_dir, sql_query, output_location)
        logger.info("SQL request generated successfully")

    except Exception as e:
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
