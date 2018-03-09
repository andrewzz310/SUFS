import xmlrpclib
from modules import nnRPCClient

class DataNode:
    def __init__(self, ip, nnIp, nnPort):
        self.blocks = []
        self.ip = ip
        self.nnRPC = nnRPCClient.nnRPCClient(nnIp, nnPort)


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


    def writeBlock(self, data, blockID, filename):
        proxy = xmlrpclib.ServerProxy("http://localhost:5000/")
        with open(filename, "wb") as handle:
            handle.write(proxy.sendFileToDN().data)