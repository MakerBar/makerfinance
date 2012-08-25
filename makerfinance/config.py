from ConfigParser import SafeConfigParser

__author__ = 'andriod'

config = None

def init(cfg_file='makerfinance.ini'):
    global config
    parser = SafeConfigParser()
    parser.read(cfg_file)
    config = parser