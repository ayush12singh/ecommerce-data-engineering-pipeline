from configparser import ConfigParser

def read_config():
    config = ConfigParser()
    config.read("/opt/airflow/config/configs.conf")
    return config
