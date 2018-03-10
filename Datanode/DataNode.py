import xmlrpclib
from modules import nnRPCClient
import os

class DataNode:
    def __init__(self, ip, nnIp, nnPort):
        self.blocks = []
        self.ip = ip
        self.nnRPC = nnRPCClient.nnRPCClient("http://" + nnIp, nnPort)


    # server to another DataNode
    def giveBlock(self, blockID, DataNodeID):
        otherdn = xmlrpclib.ServerProxy(DataNodeID)
        # string in the current blockID
        blockData = ""
        otherdn.receiveBlock(blockID, blockData)


    # client to another DataNode
    def receiveBlock(self, blockID, blockData):
        return ""
        # save new block & update report
        # 1) create a file
        # 2) write blockData to that file
        # 3) close the file
        # 4) update BlockReport (slef.listBlockID)
        # 5) send BlockReport to NameNode


    def writeBlock(self, data, blockID, IPv4, portNum):
        proxyStr = "http://" + IPv4 + ":" + portNum + "/"
        # proxy = xmlrpclib.ServerProxy("http://ec2-34-217-70-211.us-west-2.compute.amazonaws.com:8000/")
        proxy = xmlrpclib.ServerProxy(proxyStr)
        with open(blockID, "wb") as handle:
            handle.write(proxy.sendFileToDN().data)



    def removeBlock(self, blockID):
        os.remove(blockID)
        print "Successfully removed block " + blockID

