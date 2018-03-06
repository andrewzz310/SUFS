import xmlrpclib


class DataNode:
    def __init__(self):
        self.listBlockID = []
        self.ip = ""
        self.namenode = xmlrpclib.ServerProxy(self.ip)

    # server to another DataNode
    def giveBlock(self, blockID, DataNodeID):
        otherdn = xmlrpclib.ServerProxy(DataNodeID)
        # string in the current blockID
        blockData = ""
        otherdn.receiveBlock(blockID, blockData)
        return ""

    # client to another DataNode
    def receiveBlock(self, blockID, blockData):
        # save new block & update report
        # 1) create a file
        # 2) write blockData to that file
        return ""

    # server to NameNode
    def sendBlockReport(self, NameNodeID):
        # receiveBlockReport ~ whatever the name of the func in NameNode
        return self.namenode.receiveBlockReport(self.ip, self.listBlockID)




    def sendHeartBeat(self):


