import xmlrpclib


class DataNode:
    def __init__(self):
        self.listBlockID = []
        self.ip = ""
        self.namenode = xmlrpclib.ServerProxy(self.ip)

    # server to another DataNode
    def giveBlock(self, blockID, DataNodeID):

    # client to another DataNode
    def receiveBlock(self, block):

    # server to NameNode
    def sendBlockReport(self, NameNodeID):
        # receiveBlockReport ~ whatever the name of the func in NameNode
        return self.namenode.receiveBlockReport(self.ip, listBlockID)

    def sendHeartBeat(self):


