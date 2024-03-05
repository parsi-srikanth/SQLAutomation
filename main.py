import logging
from database_helper import connect_to_database, execute_sql_queries
import helper 

def main():
    config = helper.read_config()
    logging = helper.create_logger()
    logger = logging.getLogger(__name__)
    directory_path = config['SQLFiles']['DirectoryPath']
    connection_string = config['OracleDB']['connection']
    parameters = dict(config.items('parameters'))
    execute_sql_queries(directory_path, parameters, connection_string)
if __name__ == "__main__":
    main()
