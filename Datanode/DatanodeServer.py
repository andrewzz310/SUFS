"""
DatanodeServer.py
"""
from SimpleXMLRPCServer import SimpleXMLRPCServer
from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler
from DataNode import DataNode
from modules import dnRPCClient as dnRPCClient
from modules import nnRPCClient as nnRPCClient
import time
from thread import *

######### GLOBAL VARIABLES #########
PORT = 8888
HOST = ""

NAMENODE_HOST = ""
NAMENODE_PORT = "8000"

MY_IP = ""  # public ip

datanode = None  #this is the datanodes structure


# Restrict to a particular path.
class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)


# Create server
server = SimpleXMLRPCServer((HOST, PORT), requestHandler=RequestHandler)
server.register_introspection_functions()

######### HEART BEAT FUNCTIONS #########

#repeat this function every 10 seconds
def heartBeat():
    nn = nnRPCClient.nnRPCClient(NAMENODE_HOST, NAMENODE_PORT)
    while 1:
        time.sleep(10)
        nn.receiveHeartBeat(MY_IP)

#repeat this function every 10 seconds to send block report
def sendBlockReport():
    nn = nnRPCClient.nnRPCClient(NAMENODE_HOST, NAMENODE_PORT)
    while 1:
        time.sleep(10)
        #send datanodes blocks
        nn.receiveBlockReport(MY_IP, datanode.blocks)
    

######### BEGIN RPC FUNCTIONS #########

# Register a function under a different name
def hello_world():
    return "Hello, Datanode!\n"

#receive a block from anybody who calls this function
# primarily used by datanode
def receiveBlock(blockID, blockData):
    datanode.receiveBlock(blockID, blockData)
    #call datanode structure to write this to hdd
    return True


# used by namenode for targeted replications
def targetBlock(blockID, dnIp):
    targetDn = dnRPCClient.dnRPCClient(dnIp, 8888)
    blockData = "" #This needs to be replaced with a blockdata thing from our data structure
    #call datanode structure to write this to hdd
    targetDn.receiveBlock(blockID, blockData)
    return True

######### CALL THIS FUNCTION FIRST VIA NAMENODE RPC #########

#receive my ip information from namenode as well as assignment to namenode
#begin heartbeat thread
#begin block thread
#instantiates the datanode structure
def receiveNNIp(nnIp, myIp):
    print ("received namenode ip, starting heartbeats")
    global NAMENODE_HOST 
    NAMENODE_HOST = nnIp
    global MY_IP 
    MY_IP = myIp

    #instantiate datanode stuff here after we get ips
    global datanode 
    datanode = DataNode(myIp, nnIp, 8000)

    #begin heartbeating
    start_new_thread(heartBeat,())
    #begin blockreport sending
    start_new_thread(sendBlockReport,())
    return True


# Register hello world function
server.register_function(hello_world)
server.register_function(receiveBlock)
server.register_function(targetBlock)
server.register_function(receiveNNIp)
# Run the server's main loop
print("Starting Datanode Server on port " + str(PORT) + "...")
server.serve_forever()
