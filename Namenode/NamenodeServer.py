"""
NamenodeServer.py
"""
from SimpleXMLRPCServer import SimpleXMLRPCServer
from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler
from NameNode import NameNode
import time
from modules import dnRPCClient as dnRPCClient
from thread import *
import random

PORT = 8000
HOST = ""
NAMENODE_IP = ""
nn = NameNode("localhost") #instantiate so that datanodes don't die
REPLICATION = 3
RECEIVED = False

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


# blocks is a list of blockIDs
def receiveBlockReport(myIP, blocks):
    global nn

    nn.dnToBlock[myIP] = blocks
    print("\nreceive Block Report__________________________________________________")
    strList = ''
    for block in blocks:
        strList += block
        strList += '  '

    if len(blocks) == 0:
        strP = ' -------  There is no block'
    else:
        strP = ' -------  List of blocks received: ' + strList
    print('IP: ' + myIP + strP)

    for blockID in blocks: #do the translation the other way as well.
        if blockID in nn.blockD:
            if myIP not in nn.blockD[blockID]:
	            nn.blockD[blockID].append(myIP)
        else:
            nn.blockD[blockID] = [myIP]
        print('    add IP: ' + myIP + ' to dictionary blockD with blockID ' + blockID)
    print("_______________________________________________________________________\n")

    return True



def checkReplicas():
    global nn
    time.sleep(300)
    while 1:
        time.sleep(60)
        for block in nn.blockD.keys():
            if (len(nn.blockD[block]) < nn.REPLICATION):
                print (block)
                print (len(nn.blockD[block]))
                replicate(len(nn.blockD[block]), block)



def replicate(curRepFac, block):
    global nn
    rep = curRepFac
    counter = 0
    blocksrc = None
    #find first ip that can connect to
    for ip in nn.blockD[block]:
        time.sleep(10)
        try:
            blocksrc = dnRPCClient.dnRPCClient(ip, 8888)
            print ("source ip" + ip)
            print ("block to replicate: " + block)
            break
        except:
            continue

    # Either gone through all the datanodes or replication factor is met
    while (rep < nn.REPLICATION and counter < len(nn.alive)):
        ips = nn.alive.keys()
        # Shuffle live datanodes randomize placement of blocks
        random.shuffle(ips)
        for targetip in ips:
            print (targetip)
            # If the targetip doesnt have the block, it will write the block there
            if (targetip not in nn.blockD.get(block, [targetip])) and (rep < nn.REPLICATION):
                blocksrc.targetBlock(block, targetip)
                rep += 1
                print (block + " rep factor: " + str(rep))
            counter += 1



def putFile(path, filename, size):
    global nn
    #print path, filename, size
    return nn.createFile(path, filename, size) # results of namenode an



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

def lsDataNode(path):
    global nn
    return nn.lsDataNode(path)

def startHeartBeats():
    global nn
    nn.startThreads()

def printDataNodes():
    global nn
    return str(nn.alive)

#this instantiates everything
def myIp(nnip):
    global NAMENODE_IP
    NAMENODE_IP = nnip
    global nn
    nn = NameNode(nnip)
    start_new_thread(checkReplicas, ())
    return NAMENODE_IP

def getBlockReport():
    global nn
    return nn.dnToBlock

# Register hello world function
server.register_function(write1)
server.register_function(putFile)
server.register_function(createFile)
server.register_function(deleteFile)
server.register_function(mkdir)
server.register_function(deletedir)
server.register_function(ls)
server.register_function(lsDataNode)

#socketservermain calls this once dn's have been created
#maybe not, right now set it up so threads start on instantiation
server.register_function(startHeartBeats)
server.register_function(myIp)
server.register_function(getBlockReport)


# Register hello world function
server.register_function(hello_world)
server.register_function(receiveHeartBeat) #datanode calls this
server.register_function(receiveBlockReport)

#test functions
server.register_function(printDataNodes)

# Run the server's main loop
print("Staring Namenode Server on port " + str(PORT) + "...")
server.serve_forever()
