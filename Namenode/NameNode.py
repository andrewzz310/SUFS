import boto3
import socket
import sys
import time
import os
from threading import Thread, Lock

class Namenode:
    '''
    Namenode needs to do two things
    1) Which blocks are part of which files
    2) Picking N different Datanodes to store each block (then return list of blocks and datanodes)
    3) only one writer at a time so we need a lock and thread for writeFile
    '''

    #Dictionary for which blocks are part of which file
    fileD = {}
    #Dictionary for which datanodes are storing each block
    blockD = {}

    mutex = Lock()

    def __init__(self):
        self.REPLICATION = 3 #pick 3 different datanodes to store each block by default


    def writeFile(filename, blocks):  #pass in array of blocks as arguments

        uniqueFile = filename
        #need to find out how to make 'uniqueFile' the name of the file otherwise dictionary overwrites itself everytime method is called
        fileD['uniqueFile'] = blocks

        '''
        For each block from file, we need to apply replication factor 
        '''
        # For every element in blocks, part of key<uniqueFile>, Value<blocks>
                #place the block into N different datanodes either by default or updated REPLICATION
                 blockD = {blockName, datanodes}

        #return list of blocks and datanodes back to client


    #The block report given from the data node
    def blockReport(datanodeNum, blocks ): #pass in all blocks as array assigned to the specific datanodeNumber e.g. datanode1,datanode2,etc





