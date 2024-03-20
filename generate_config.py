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
        'dbname': '',
        'user': '',
        'password': '',
        'host': '',
        'port': '',
        'connection': 'SYSTEM/qwerty123@localhost:1521/XE',
        'sqlalchemy_connection': 'oracle+cx_oracle://SYSTEM:qwerty123@localhost:1521/XE'
    }

    # SQL Files directory configuration
    config['SQLFiles'] = {
        'DirectoryPath': './sql_files'
    }

    # Parameters configuration
    config['parameters'] = {
        'dbname': '',
        'user': '',
        'password': '',
        'numrec2': '2',
        'numrec': '1',
        'tablename': 'transactions'
    }

    # Directories configuration
    config['Directories'] = {
        'Incoming': 'X:/Student Worker Files/Parsi/SQLAutomation/Incoming_Requests/',
        'Processed': 'X:/Student Worker Files/Parsi/SQLAutomation/Processed_Requests/',
        'Results': 'X:/Student Worker Files/Parsi/SQLAutomation/Results/'
    }

    with open('configurations.ini', 'w') as configfile:
        config.write(configfile)

if __name__ == "__main__":
    generate_config()
