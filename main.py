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
        file.write(f"-- Metadata: Output Location: {output_location}\n")
        file.write(sql_query)

def main():
    config = helper.read_config()
    logging = helper.create_logger()
    logger = logging.getLogger(__name__)
    directory_path = config['SQLFiles']['DirectoryPath']
    connection_string = config['OracleDB']['connection']
    parameters = dict(config.items('parameters'))
    outgoing_dir = 'outgoing_requests/'
    output_location = "results/"
    
    env = Environment(loader = FileSystemLoader('sql_files', followlinks=True), trim_blocks=True, lstrip_blocks=True)

    env.globals['tableName'] = parameters['tablename']
    env.globals['ids'] = (1, 2, 3)
    template = env.get_template('HispanicStudents/HispanicStudents.sql')

    sql_query = template.render()

    print(sql_query)

    generate_sql_request(outgoing_dir, sql_query, output_location)

if __name__ == "__main__":
    main()
