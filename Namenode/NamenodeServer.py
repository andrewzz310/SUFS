"""
NamenodeServer.py
"""
from SimpleXMLRPCServer import SimpleXMLRPCServer
from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler
from NameNode import NameNode
import time

PORT = 8000
HOST = "localhost"

nn = NameNode()

# Restrict to a particular path.
class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)


# Create server
server = SimpleXMLRPCServer((HOST, PORT), requestHandler=RequestHandler)
server.register_introspection_functions()


# Register a function under a different name
def hello_world():
    return "Hello, Namenode!\n"


def write1(filename, size):
    print (filename, " ", size)
    blocks = nn.addFile(filename, size)
    return str(blocks)


def receiveHeartBeat(myIp):
    #where myIP is the datanodes IP (so you can access the RPC stuff)
    nn.alive[myIp] = time.time()
    return True


def receiveBlockReport(myIp, blocks):
    nn.blockD[myIp] = blocks
    for blockID in blocks: #do the translation the other way as well.
        nn.dnToBlock[blockID].add(myIp)
    return True


def putFile(filename, size):
    print (filename)
    print (size)
    return # results of namenode an


# Directory functions
def ls(path):
    return nn.ls(path)


def mkdir(path, dir):
    nn.mkdir(path, dir)
    return True


def deletedir(path):
    return nn.deleteDirectory(path)


# Register hello world function
server.register_function(write1)
server.register_function(putFile)
server.register_function(ls)
server.register_function(mkdir)
server.register_function(deletedir)


# Register hello world function
server.register_function(hello_world)
server.register_function(receiveHeartBeat) #datanode calls this

# Run the server's main loop
print("Staring Namenode Server on port " + str(PORT) + "...")
server.serve_forever()
