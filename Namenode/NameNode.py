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

        # Dictionary of Datanodes for easy lookup
        # {key : value} = {DataNode1 : [blockID, blockID], DataNode2 : [blockID, blockID], ...}
        self.dnToBlock = {}

        self.mutex = Lock()

        # Dictionary of path(s) and their content(s)
        # {key : value} = {/home/ : [file1, dir1], ...}
        self.contentsInDir = {"/home/": []}

        self.startThreads()
        self.ip = myIp
        #self.block_size = 4000000  # 4MB
        self.block_size = 64000000  # in bytes aka 64MB
        self.dn_assign_counter = 0  # used to assign blocks to datanodes ex: self.dn_assign_counter % <number of DNs>
        #self.restore() # If the NameNode gets rebooted, it picks up where it left off.



    def nameNodeDisk(self):
        # write the Directory System like a Real File System on the NameNode's disk
        outFile = open('contentsInDir.txt', 'w')
        for path, contents in self.contentsInDir.iteritems():
            outFile.write(path)
            outFile.write('\n')

            # write how many files and directories in path
            size = len(contents)
            strSize = str(size)
            outFile.write(strSize)  # write the length of the contents in the current path
            outFile.write('\n')

            for file in contents:
                strFile = file
                outFile.write(strFile)
                outFile.write('\n')
        outFile.close()

        print('Path to contentsInDir.txt and fileD.txt', os.path.dirname(os.path.abspath(__file__)))

        outFile2 = open('fileD.txt', 'w')
        for pathfilename, listBlockIDs in self.fileD.iteritems():
            outFile2.write(pathfilename)
            outFile2.write('\n')

            # write how many blockIDs in path+filename
            size = len(listBlockIDs)
            strSize = str(size)
            outFile2.write(strSize)  # write the length of the blockIDs in the path+filename
            outFile2.write('\n')

            for blockID in listBlockIDs:
                strFile = blockID
                outFile2.write(strFile)
                outFile2.write('\n')
        outFile2.close()



    # If the NameNode gets rebooted, it picks up where it left off.
    def restore(self):
        # if the file doesn't exist, there is nothing in Namenode
        if os.path.isfile('contentsInDir.txt') == False:
            return

        if os.stat('contentsInDir.txt').st_size == 0:
            return

        outFile = open('contentsInDir.txt', 'r')

        while outFile.read(1):
            path = outFile.readline()
            path = path.replace('\n', '')
            strSize = outFile.readline()
            strSize = strSize.replace('\n', '')
            size = 0#int(strSize)

            if size > 0:
                for i in range (0, size-1):
                    file = outFile.readline()
                    file = file.replace('\n','')
                    self.contentsInDir[path].append(file)
        outFile.close()

        outFile = open('fileD.txt', 'r')

        while outFile.read(1):
            pathfilename = outFile.readline()
            pathfilename = pathfilename.replace('\n', '')
            strSize = outFile.readline()
            strSize = strSize.replace('\n', '')
            print('_' + strSize + '_')
            size = 0 #int(strSize) 

            if size > 0:
                for i in range(0, size - 1):
                    blockID = outFile.readline()
                    blockID = blockID.replace('\n', '')
                    self.fileD[pathfilename].append(blockID)
        outFile.close()



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
            # Add file and its blockIDS
            self.fileD[path+filename] = listBlockID
            self.nameNodeDisk()
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

                self.removeItemInBlockD_dnToBlock(retDict)
                del self.fileD[path+filename]   # Remove file and its blockIDS
                self.nameNodeDisk()
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
            if dir in self.contentsInDir[path]:
                return 'Directory exists'
            self.contentsInDir[path + dir + "/"] = []
            self.contentsInDir[path].append(dir)
            self.nameNodeDisk()
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
        self.nameNodeDisk()
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

        return "No such directory"



    # List the DataNodes that store replicas of each block of a file
    # Given a file path, it returns a dictionary with blockID as key
    # and  a list of DataNodes that hold replicas as value
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



######### fault tolerance stuff #########

    #start threads
    def startThreads(self):
        start_new_thread(self.checkTimes, ())



    def checkTimes(self):
        while 1:
            time.sleep(10)
            for ip in self.alive.keys():
                diff = time.time() - self.alive[ip]
                if diff > 20:
                    print("dead ip is: " + ip)
                    #sys.stdout=open("/home/ec2-user/test.txt","w")
                    print ("create new datanode")
                    self.createNewDN(ip)
                    print ("deleting from blockreport")
                    self.deleteFromBlockReport(ip)
                    print("removing from membership")
                    del self.alive[ip]
                    del self.dnToBlock[ip]
                    #sys.stdout.close()



    def deleteFromBlockReport(self, dnIp):
        for block in self.dnToBlock.get(dnIp, []):
            for ip in self.blockD.get(block, []):
                if ip == dnIp:
                    self.blockD[block].remove(dnIp)
        


    def createNewDN(self, prevDNIp):
        print ('***************************************')
        print ('***************************************')
        print ('***************************************')
        print('creating ec2 instance')
        print ('***************************************')
        print ('***************************************')
        print ('***************************************')

        ec2 = boto3.resource('ec2')
        instance_id = ''
        instance_check = None
        instance = ec2.create_instances(
        ImageId = 'ami-9263f0ea',
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
        time.sleep(60)
        
        dnIp = str(instance_check.public_ip_address)
        print ('***************************************')
        print ('***************************************')
        print ('***************************************')
        print (dnIp)
        print ('***************************************')
        print ('***************************************')
        print ('***************************************')
        datanode = xmlrpclib.ServerProxy("http://" + dnIp + ':' + '8888')
        print ('connected to dn')
        datanode.receiveNNIp("http://" + self.ip, "http://" + dnIp)
        print ('heartbeat started on ' + dnIp)
        print ('name node is ' + self.ip)
        print('moving blocks..')
        self.moveBlocks(dnIp, prevDNIp)
        print('blocks moved')



    def moveBlocks(self, targetDNIp, prevDNIp):
        for block in self.dnToBlock.get(prevDNIp):
            for ip in self.blockD.get(block):
                if (ip != prevDNIp):
                    datanode = dnRPCClient.dnRPCClient(ip, 8888)
                    success = datanode.targetBlock(block, "http://" + targetDNIp)
                    if (success):
                        print('found the block! breaking...')
                        break
        return True

