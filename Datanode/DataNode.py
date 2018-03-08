import xmlrpclib
from modules import nnRPCClient


class DataNode:
    def __init__(self, ip, nnIp, nnPort):
        self.blocks = []
        self.ip = ip
        self.nnRPC = nnRPCClient.nnRPCClient(nnIp, nnPort)


    # server to another DataNode
    # probably won't be used
    # definitely don't use this
    def giveBlock(self, blockID, DataNodeID):
        otherdn = xmlrpclib.ServerProxy(DataNodeID)
        # string in the current blockID
        blockData = ""
        otherdn.receiveBlock(blockID, blockData)


    # client to another DataNode
    # probably won't be used
    # definitely don't use this
    def receiveBlock(self, blockID, blockData):
        return ""
        # save new block & update report
        # 1) create a file
        # 2) write blockData to that file
        # 3) close the file
        # 4) update BlockReport (slef.listBlockID)
        # 5) send BlockReport to NameNode


    # server to NameNode
    def sendBlockReport(self):
        # receiveBlockReport ~ whatever the name of the func in NameNode
        return self.nnRPC.receiveBlockReport(self.ip, self.blocks)

