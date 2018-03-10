"""
NamenodeServer.py
"""
from SimpleXMLRPCServer import SimpleXMLRPCServer
from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler
from NameNode import NameNode
import time

PORT = 8000
HOST = ""
NAMENODE_IP = ""
nn = None


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
    #missing function for writing?
    blocks = None #nn.addFile(filename, size) 
    return str(blocks)


def receiveHeartBeat(myIp):
    global nn
    #where myIP is the datanodes IP (so you can access the RPC stuff)
    nn.alive[myIp] = time.time()
    return True


def receiveBlockReport(myIp, blocks):
    global nn
    nn.blockD[myIp] = blocks
    for blockID in blocks: #do the translation the other way as well.
        nn.dnToBlock[blockID].add(myIp)
    return True


def putFile(path, filename, size):
    global nn
    print path, filename, size
    nn.createFile(path, filename, size)
    return []  # results of namenode an


# Directory functions
def createFile(path, filename):
    global nn
    return nn.createFile(path, filename)


def deleteFile(path, filename):
    global nn
    return nn.deleteFile(path, filename)


def mkdir(path, dir):
    global nn
    return nn.mkdir(path, dir)


def deletedir(path):
    global nn
    return nn.deleteDirectory(path)


def ls(path):
    global nn
    return nn.ls(path)


def startHeartBeats():
    global nn
    nn.startThreads()


def printDataNodes():
    global nn
    return str(nn.alive)


def myIp(nnip):
    global NAMENODE_IP
    NAMENODE_IP = nnip
    global nn
    nn = NameNode(NAMENODE_IP)
    return NAMENODE_IP


# Register hello world function
server.register_function(write1)
server.register_function(putFile)
server.register_function(createFile)
server.register_function(deleteFile)
server.register_function(mkdir)
server.register_function(deletedir)
server.register_function(ls)

#socketservermain calls this once dn's have been created
#maybe not, right now set it up so threads start on instantiation
server.register_function(startHeartBeats)
server.register_function(myIp)


# Register hello world function
server.register_function(hello_world)
server.register_function(receiveHeartBeat) #datanode calls this
server.register_function(receiveBlockReport)

#test functions
server.register_function(printDataNodes)

# Run the server's main loop
print("Staring Namenode Server on port " + str(PORT) + "...")
server.serve_forever()
