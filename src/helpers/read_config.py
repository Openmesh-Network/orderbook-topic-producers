from configparser import ConfigParser

CONFIG_PATH = "config.ini"

def get_consumer_config():
    config = ConfigParser()
    config.read(CONFIG_PATH)
    return dict(config['CONSUMER'])

def get_producer_config():
    config = ConfigParser()
    config.read(CONFIG_PATH)
    return dict(config['PRODUCER'])