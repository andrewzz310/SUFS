import xmlrpclib
from modules import nnRPCClient
import os


class DataNode:
    def __init__(self, ip, nnIp, nnPort):
        self.blocks = []
        self.ip = ip
        self.nnRPC = nnRPCClient.nnRPCClient("http://" + nnIp, nnPort)
        self.block_dir = '/Users/justin/cs/cloud/SUFS/blocks/'#'/home/ec2-user/blocks/'#'/Users/justin/cs/cloud/SUFS/blocks/'
        if not os.path.exists(self.block_dir):
            os.makedirs(self.block_dir)


    # server to another DataNode
    #def giveBlock(self, blockID, DataNodeID):
    def giveBlock(self, blockID):
        #otherdn = xmlrpclib.ServerProxy(DataNodeID)
        # string in the current blockID
        block_obj = None
        with open(self.block_dir+blockID, "rb") as handle:
            block_obj = xmlrpclib.Binary(handle.read())
        return block_obj


    # client to DataNode
    # save new block & update report
    # 1) create a file
    # 2) write blockData to that file
    # 3) close the file
    # 4) update BlockReport (slef.listBlockID)
    # 5) send BlockReport to NameNode
    def receiveBlock(self, blockID, blockData):
        with open(self.block_dir+blockID, "wb") as handle:
            handle.write(blockData.data)

        self.blocks.append(blockID)
        return True


    def removeBlock(self, blockID):
        if os.path.isfile(self.block_dir+blockID):
            os.remove(self.block_dir+blockID)
            self.blocks.remove(blockID)
            print "Successfully removed block " + blockID
            return True
        else:
            print "File not found at " + self.block_dir + blockID
            return False

