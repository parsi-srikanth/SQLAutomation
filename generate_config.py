import configparser

def generate_config():
    config = configparser.ConfigParser()

    # Logger configuration
    config['Logger'] = {
        'LogFilePath': './',
        'LogFileName': 'sql_query.log',
        'LogLevel': 'INFO'
    }

    # Database configuration
    config['OracleDB'] = {
        'user': 'SYSTEM',
        'password': 'qwerty123',
        'host': 'localhost',
        'port': '1521',
        'service_name': 'XE',
        'InstantClientPath': 'C:/oracle/instantclient_19_9'
        
    }

    # SQL Files directory configuration
    config['SQLFiles'] = {
        'DirectoryPath': './sql_files' # update this path to the directory where the SQL files are stored and modify the path in the client.py file line 67
    }

    # Parameters configuration
    config['parameters'] = {
        'tablename': 'transactions'
    }

    config['client'] = {
        'requestor': 'Parsi',
        'process_status': 'Pending',
        'sql_template': 'HispanicStudents/HispanicStudents.sql',
        'output_location': 'X:/Student Worker Files/Parsi/SQLAutomation/Results/'
    }

    # Directories configuration
    config['Directories'] = {
        'Failed': 'X:/Student Worker Files/Parsi/SQLAutomation/Failed_Requests/',
        'Incoming': 'X:/Student Worker Files/Parsi/SQLAutomation/Incoming_Requests/',
        'Processed': 'X:/Student Worker Files/Parsi/SQLAutomation/Processed_Requests/',
        'Results': 'X:/Student Worker Files/Parsi/SQLAutomation/Results/'
    }

    with open('configurations.ini', 'w') as configfile:
        config.write(configfile)

if __name__ == "__main__":
    generate_config()
