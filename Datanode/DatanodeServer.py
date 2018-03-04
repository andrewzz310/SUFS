"""
DatanodeServer.py
"""
from SimpleXMLRPCServer import SimpleXMLRPCServer
from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler
from DataNode import DataNode
from modules import dnRPCClient as dnRPCClient
from modules import nnRPCClient as nnRPCClient

PORT = 8880
HOST = "localhost"

NAMENODE_HOST = "something"
NAMENODE_PORT = "1234"

# Restrict to a particular path.
class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)


# Create server
server = SimpleXMLRPCServer((HOST, PORT), requestHandler=RequestHandler)
server.register_introspection_functions()

datanode = DataNode(HOST, NAMENODE_HOST, NAMENODE_PORT)


# Register a function under a different name
def hello_world():
    return "Hello, Datanode!\n"


def receiveBlock(blockID, blockData):
    datanode.receiveBlock(blockID, blockData)
    return True

# used by namenode for targeted replications
def targetBlock(blockID, dnIp, port):
    targetDn = dnRPCClient.dnRPCClient(dnIp, port)
    blockData = "" #This needs to be replaced with a blockdata thing from our data structure
    targetDn.receiveBlock(blockID, blockData)
    return True


# Register hello world function
server.register_function(hello_world)
server.register_function(receiveBlock)
server.register_function(targetBlock)
# Run the server's main loop
print("Staring Datanode Server on port " + str(PORT) + "...")
server.serve_forever()
