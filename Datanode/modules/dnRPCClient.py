"""
RPCClient.py
"""
import xmlrpclib


class dnRPCClient:

    def __init__(self, url, port):
        self.port = port
        self.url = url
        self.server = xmlrpclib.ServerProxy(self.url + ':' + str(self.port))

    def hello_world(self):
        return self.server.hello_world()

    def receiveBlock(self, blockID, blockData):
        return self.server.receiveBlock(blockID, blockData)
    
    def targetBlock(self, blockID, blockData):
        return self.server.targetBlock(blockID, blockData)
"""
Example of us
"""
#rc = RPCClient("http://localhost", 8000)
#print rc.get_hello_world()
