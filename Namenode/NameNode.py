import boto3
import socket
import sys
import time
import os
import xmlrpclib
from threading import Thread, Lock
from SimpleXMLRPCServer import SimpleXMLRPCServer
from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler

class NameNode:
    """
    Namenode needs to do two things
    1) Which blocks are part of which files
    2) Picking N different Datanodes to store each block (then return list of blocks and datanodes)
    3) only one writer at a time so we need a lock and thread for writeFile
    """
    def __init__(self):
        self.REPLICATION = 3 # pick 3 different datanodes to store each block by default
        self.fileD = {} # Dictionary for which blocks are part of which file
        self.blockD = {} # Dictionary for which datanodes are storing each block
        self.alive = {} # Dict for alive datanodes
        self.mutex = Lock()



    def writeFile(self, filename, blocks):  #pass in array of blocks as arguments

        uniqueFile = filename
        #need to find out how to make 'uniqueFile' the name of the file otherwise dictionary overwrites itself everytime method is called
        self.fileD['uniqueFile'] = blocks

        '''
        For each block from file, we need to apply replication factor 
        '''
        # For every element in blocks, part of key<uniqueFile>, Value<blocks>
        #place the block into N different datanodes either by default or updated REPLICATION
        #blockD = {blockName, datanodes}

        #return list of blocks and datanodes back to client


    # def blockReport(self, datanodeNum, blocks ):
    #     """
    #     The block report given from the data node
    #     Pass in all blocks as array assigned to the specific datanodeNumber e.g. datanode1,datanode2,etc
    #     :param datanodeNum:
    #     :param blocks:
    #     :return:
    #     """
    #     blockManager = xmlrpclib.ServerProxy('http://localhost:5000')
    #     print blockManager.get_blockID()
    #     print blockManager.get_DataNodeNumber()


    def blockReport(self, datanodeNum, blocks ):
        """
        The block report given from the data node
        Pass in all blocks as array assigned to the specific datanodeNumber e.g. datanode1,datanode2,etc
        :param datanodeNum:
        :param blocks:
        :return:
        """
        #blockManager = xmlrpclib.ServerProxy('http://localhost:5000')
        #print blockManager.get_blockID()
        #print blockManager.get_DataNodeNumber()


    def checkTimes(self):
        for key in self.alive.keys():
            diff = time.time() - self.alive[key]
            if (diff > 10):
                del self.alive[key]

# for testing

# s = Namenode()
# s.blockReport( 1, 2)

#s = Namenode()
#s.blockReport( 1, 2)

