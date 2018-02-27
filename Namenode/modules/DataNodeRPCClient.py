"""
RPCClient.py
"""
import xmlrpclib


class DataNodeRPCClient:

    def __init__(self, url, port):
        self.port = port
        self.url = url
        self.server = xmlrpclib.ServerProxy(self.url + ':' + str(self.port))

    def hello_world(self):
        return self.server.hello_world()

    def ekg(self):
        return self.server.ekg()

"""
Example of us
"""
#rc = RPCClient("http://localhost", 8000)
#print rc.get_hello_world()
