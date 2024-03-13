import logging
from database_helper import connect_to_database, execute_sql_queries, connect_and_execute_query
import helper 
from jinja2 import Environment, FileSystemLoader

def main():
    config = helper.read_config()
    logging = helper.create_logger()
    logger = logging.getLogger(__name__)
    directory_path = config['SQLFiles']['DirectoryPath']
    connection_string = config['OracleDB']['connection']
    #connection_string = config['OracleDB']['sqlalchemy_connection']
    parameters = dict(config.items('parameters'))

    env = Environment(loader = FileSystemLoader('sql_files/Code4'))

    template = env.get_template('HispanicStudents.sql')

    output = template.render(tableName = parameters['tablename'])

    print(output)
    print(connect_and_execute_query(connection_string, output))

    #execute_sql_queries(directory_path, parameters, connection_string)
if __name__ == "__main__":
    main()
