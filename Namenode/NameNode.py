import boto3
import socket
import sys
import time
import math
import os
import xmlrpclib
from threading import Thread, Lock
from thread import *
from SimpleXMLRPCServer import SimpleXMLRPCServer
from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler
import re
from modules import dnRPCClient
import collections


# NOTE:  The '#' is not allowed in filename because it's used for blockID stuff
reservedChar = ["#", "<", ">", ":", "/", "\\", "|", "?", "*"]

class NameNode:
    """
    Namenode needs to do two things
    1) Which blocks are part of which files
    2) Picking N different Datanodes to store each block (then return list of blocks and datanodes)
    3) only one writer at a time so we need a lock and thread for writeFile
    """
    def __init__(self, myIp):
        self.REPLICATION = 3 # pick 3 different datanodes to store each block by default

        # Dictionary for which blocks are part of which file
        # {key : value} = {path+filename : [blockID, blockID, blockID, ...]}
        self.fileD = {}

        # Dictionary for which datanodes are storing each block
        # {key : value} = {blockID : [DataNode1, DataNode2, DataNode3]}
        self.blockD = {}

        self.alive = {}  # Dict for alive datanodes, <key: datanodeIP, value: timestamp>

        # Dictinary of Datanodes for easy lookup
        # {key : value} = {DataNode1 : [blockID, blockID], DataNode2 : [blockID, blockID], ...}
        self.dnToBlock = {}

        self.mutex = Lock()
        self.contentsInDir = {"/home/": []}
        self.startThreads()
        self.ip = myIp
        self.block_size = 256000000  # in bytes aka 256MB
        self.dn_assign_counter = 0  # used to assign blocks to datanodes ex: self.dn_assign_counter % <number of DNs>



    # Create a file
    # Example of how to call the function:      createFile("/home/st/", "text1.txt", 983)
    # returns list of (blockID, DatanodeIP)
    def createFile(self, path, filename, filesize):
        result = list()
        path_hash = path.replace('/', '#')
        block_base_name = path_hash + filename + '.part'
        num_of_datanodes = len(self.alive)
        block_num = 1
        current_size = 0

        # Check if the filename is valid. This prevents causing Exception on ec2 instance
        if self.checkValidFile(path, filename):
            listBlockID = []
            while current_size < filesize:
                print('Block: ' + block_base_name + str(block_num))
                dn_index = self.dn_assign_counter % num_of_datanodes
                dn_ip = self.alive.keys()[dn_index]
                blockID = block_base_name+str(block_num)
                result.append((blockID, dn_ip))
                print('Added ' + blockID + ' to list!')
                self.dn_assign_counter += 1
                current_size += self.block_size
                block_num += 1
                listBlockID.append(blockID)
            # Add file to Directory
            self.contentsInDir[path].append(filename)

            self.fileD[path+filename] = listBlockID

        return result



    # checkFile("/home/st/", "text1.txt")
    # returns True if ok to add file else False
    def checkValidFile(self, path, filename):
        # Check if the filename is valid.  This prevents causing Exception on ec2 instance
        # NOTE:  The '#' is not allowed in filename because it's used for blockID stuff
        if re.match("^[\w,\s-]+[\.[A-Za-z]+]*$", filename):
            if path in self.contentsInDir:
                if file in self.contentsInDir[path]:
                    return False  # File already exists
                else:
                    # when the file is created, an S3 object should be specified
                    # and the data from S3 should be written into the file__________________________
                    return True
            else:
                return False  # Fail to create a file because the directory doesn't exist
        else:
            print('wrong file format!')
            return False  # Invalid filename



    # Update self.blockD and self.dnToBlock after calling deleteFile() or deleteDirectory
    def removeItemInBlockD_dnToBlock(self, dictionary):
        for blockID, listDN in dictionary.iteritems():
            #remove the key-value pair of blockID in dictionary blockD
            del self.blockD[blockID]

            print "before for-loop"
            print('dnToBlock')
            for datanode in listDN:
                print('++++')
                print('dnToBlock[datanode]', self.dnToBlock[datanode])
                print('listDN', listDN)
                print "blockID to delete " + blockID

                while blockID in self.dnToBlock[datanode]:
                    print '+++++++ removed ' + blockID + ' from ' + datanode
                    self.dnToBlock[datanode].remove(blockID)



    # Delete a file
    # Example of how to call the function:      deleteFile("/home/st/", "text.txt")
    # Return the dictionary with key is blockID and value is a list of DataNodes
    #        and the ClientServer will connect with those DataNodes to delete blocks
    def deleteFile(self, path, filename):
        if path in self.contentsInDir:
            if filename in self.contentsInDir[path]:
                self.contentsInDir[path].remove(filename)

                # delete the file (blocks) in DataNodes________________________________________
                retDict = self.lsDataNode(path + filename)

                print "before removeItemInBlockD_dnToBlock"
                self.removeItemInBlockD_dnToBlock(retDict)
                return retDict
            else:
                print('File doesn\'t exist')
                return {}  # File doesn't exist
        else:
            print('No such directory')
            return {}  # No such directory



    # Create a directory
    # Example of how to call the function:   mkdir("/home/", "st")
    def mkdir(self, path, dir):
        if any(char in dir for char in reservedChar):
            return "Name of directory cannot any of reserved characters"
        if path in self.contentsInDir:
            self.contentsInDir[path + dir + "/"] = []
            self.contentsInDir[path].append(dir)
            return "Successfully created a directory"
        else:
            return "Fail to create a directory"



    # Example of how to call the function:     deleteDirectory("/home/st/")
    def deleteDirectory(self, path):
        if path == "/home/":
            return "You can't delete the root folder"
        if path in self.contentsInDir:
            # 1. look at the list and delete all the files___________NEED TO TEST
            #    Note: Ignore if there is a sub-directory, it will be delete in the for loop
            retDict = {}

            # if the directory is empty, return here
            if len(self.contentsInDir[path]) == 0:
                del self.contentsInDir[path]
                return retDict

            print("List of files need to delete: ", self.contentsInDir[path])

            for file in self.contentsInDir[path]:
                # check if it is a directory or file
                if re.match("^[\w,\s-]+[\.[A-Za-z]+]*$", file):
                    retDict.update(self.lsDataNode(path + file))
            del self.contentsInDir[path]

            # 2. check if there is sub-directory in the current "path"
            #    If there is, delete it.
            for key in self.contentsInDir.keys():
                if path in key:
                    # 3. look at the list and delete all the files___________NEED TO TEST
                    print("List of files need to delete: ", self.contentsInDir[key])
                    for file in self.contentsInDir[key]:
                        # check if it is a directory or file
                        if re.match("^[\w,\s-]+[\.[A-Za-z]+]*$", file):
                            retDict.update(self.lsDataNode(key + file))

                    del self.contentsInDir[key]

            # 4. Remove the directory from parent directory
            index = 0
            # Find the parent path
            for i, val in enumerate(path[:-1]):  # exclude the last "/"
                if val == "/":
                    index = i

            parentDir = path[: index+1]
            delDirName = path[index+1 : len(path)-1]
            self.contentsInDir[parentDir].remove(delDirName)

        self.removeItemInBlockD_dnToBlock(retDict)
        return retDict



    # List the contents of a directory
    # Example of how to call the function:     ls("/home/")
    def ls(self, path):
        result = list()
        for key in self.contentsInDir:
            if key == path:
                for content in self.contentsInDir[key]:
                    print(content)
                    result.append(content)
        return result



    # List the DataNodes that store replicas of each block of a file
    # Given a file path, the NameNode returns a list of blocks for that file
    # and a list of DataNodes that hold replicas for each list
    # Example of how to call the function:      lsDataNode("/home/text.txt")
    # Return a dictionary:      {key : value}
    #                           {blockID, [DataNode1, DataNode2, DataNode3]}
    #                           {blockID, [DataNode3, DataNode4, DataNode5]}
    def lsDataNode(self, pathfilename):
        retDict = {}
        #blockIDlist = []

        if pathfilename in self.fileD:
            blockIDlist = self.fileD[pathfilename]
            print('blockIDlist', blockIDlist)
            # and a list of DataNodes that hold replicas for each list
            for blockID in blockIDlist:
                if blockID in self.blockD:
                    retDict[blockID] = self.blockD[blockID]
        return retDict



    # def writeFile(self, filename, blocks):  #pass in array of blocks as arguments
    #
    #     uniqueFile = filename
    #     #need to find out how to make 'uniqueFile' the name of the file otherwise dictionary overwrites itself everytime method is called
    #     self.fileD['uniqueFile'] = blocks
    #
    #     '''
    #     For each block from file, we need to apply replication factor
    #     '''
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



######### Alex's fault tolerance stuff #########

    #start threads
    def startThreads(self):
        start_new_thread(self.checkTimes, ())



    def checkTimes(self):
        while 1:
            time.sleep(30)
            for ip in self.alive.keys():
                diff = time.time() - self.alive[ip]
                if diff > 40:
                    #sys.stdout=open("/home/ec2-user/test.txt","w")
                    print ("create new datanode")
                    self.createNewDN(ip)
                    print ("deleting from blockreport")
                    self.deleteFromBlockReport(ip)
                    print("removing from membership")
                    del self.alive[ip]
                    #sys.stdout.close()



    def deleteFromBlockReport(self, dnIp):
        for block in self.dnToBlock.get(dnIp, []):
            for ip in self.blockD.get(block, []):
                if ip == dnIp:
                    self.blockD[block].remove(dnIp)
        


    def createNewDN(self, prevDNIp):
        print('creating ec2 instance')
        ec2 = boto3.resource('ec2')
        instance_id = ''
        instance_check = None
        instance = ec2.create_instances(
        ImageId = 'ami-0beb7a73',
        MinCount = 1,
        MaxCount = 1,
        InstanceType='t2.micro',
        )
        instance_id = instance[0].id
        print('Created Datanode Server:', instance[0].id, instance[0].public_ip_address)
        instance_check = instance[0]
        print('Getting Public IP...')

        # Wait for server to
        while instance_check.public_ip_address == None:
            time.sleep(10)
            instance_check = ec2.Instance(instance_id)
        print('Waiting for bootup')
        time.sleep(40)
        
        dnIp = str(instance_check.public_ip_address)
        print (dnIp)
        datanode = xmlrpclib.ServerProxy("http://" + dnIp + ':' + '8888')
        print ('connected to dn')
        datanode.receiveNNIp("http://" + self.ip, "http://" + dnIp)
        print ('heartbeat started on ' + dnIp)
        print ('name node is ' + self.ip)
        print('moving blocks..')
        self.moveBlocks(dnIp, prevDNIp)
        print('blocks moved')



    def moveBlocks(self, targetDNIp, prevDNIp):
        for block in self.dnToBlock.get(prevDNIp, []):
            for ip in self.blockD.get(block, []):
                try:
                    datanode = dnRPCClient.dnRPCClient(ip, 8888)
                    success = datanode.targetBlock(block, targetDNIp)
                    if (success):
                        print('found the block! breaking...')
                        break
                except:
                    continue
        return True

# for testing
# s = Namenode()
# s.blockReport( 1, 2)
