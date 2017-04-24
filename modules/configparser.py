import ConfigParser
import os
import socket

def getsettings(section):
    config = ConfigParser.ConfigParser()
    root_path = os.getcwd()
    if 'elementalled.com' in socket.gethostname():
        config.read('/home/imcfarlane/pythondev/config/config.ini')
    else:
        config.read('/var/local/dev/pythondev/config/config.ini')
    
    dict1 = {}  
    options = config.options(section)
    for option in options:
        try:
            dict1[option] = config.get(section, option)
            if dict1[option] == -1:
                DebugPrint("skip: %s" % option)
        except:
            print("exception on %s!" % option)
            dict1[option] = None
    return dict1