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
        self.dnToBlock = {}
        self.mutex = Lock()
        self.contentsInDir = {"/home/": []}


    # Create a directory
    def mkdir(self, path, dir):
        if path in self.contentsInDir:
            self.contentsInDir[path + dir + "/"] = []
            self.contentsInDir[path].append(dir)
            return "Successfully created a directory"
        else:
            return "Fail to create a directory"



    def deleteDirectory(self, path):
        if path in self.contentsInDir:
            # 1. look at the list and delete all the files_________________________________
            #    Note: Ignore if there is a sub-directory, it will be delete in the for loop
            print("List of files need to delete: ", self.contentsInDir[path])
            del self.contentsInDir[path]

            # 2. check if there is a directory under this current path
            #    If there is, delete that to
            for key in self.contentsInDir.keys():
                if path in key:
                    # 3. look at the list and delete all the files__________________________
                    print("List of files need to delete: ", self.contentsInDir[key])
                    del self.contentsInDir[key]



    # List the contents of a directory
    def ls(self, path):
        for key in self.contentsInDir:
            for content in self.contentsInDir[key]:
                print(content)

# mkdir("/home/", "hang")
# printDic()
# print("")
#
# mkdir("/home/hang/", "nguyen")
# printDic()
# print("")
#
# deleteDirectory("/home/hang/")
# printDic()
# print("")
#
# ls("/home/")


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


    def checkTimes(self):
        for key in self.alive.keys():
            diff = time.time() - self.alive[key]
            if (diff > 10):
                del self.alive[key]

    def checkReplicas(self):
        notRep = [] #structure that holds
        for blockID in self.dnToBlock.keys():
            if (len(self.dnToBlock[blockID]) != self.REPLICATION):
                notRep.append(blockID)
        return notRep

# for testing
# s = Namenode()
# s.blockReport( 1, 2)
