"""
Simple socket server using multiple threads
"""

from __future__ import print_function
import socket
import sys
import boto3
import botocore
import xmlrpclib
import subprocess
import os
import time
from thread import *
import modules.BlockDivider as BlockDivider
import Client


# For RPC client interactions
RPC_NAMENODE_SERVER_URL = ''
rpc_namenode = None

HOST = ''    # Symbolic name meaning all available interfaces
PORT = 8888  # Arbitrary non-privileged port
NAMENODE_IP = ''

# Client
client = Client.Client()

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
print('Socket created')

# Bind socket to local host and port
try:
    s.bind((HOST, PORT))
except socket.error as msg:
    print('Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1])
    sys.exit()

print('Socket bind complete')

# Start listening on socket
s.listen(10)
print('Server now listening')


def create_ec2():
    ec2 = boto3.resource('ec2')
    instance_id = ''
    instance_check = None
    instance = ec2.create_instances(
        ImageId='ami-daeb7da2',
        MinCount=1,
        MaxCount=1,
        InstanceType='t2.micro',
    )
    instance_id = instance[0].id
    print('Created Namenode Server:', instance[0].id, instance[0].public_ip_address)

    instance_check = instance[0]

    print('Getting Public IP...')

    # Wait for server to
    while instance_check.public_ip_address == None:
        instance_check = ec2.Instance(instance_id)

    RPC_NAMENODE_SERVER_URL = 'http://' + str(instance_check.public_ip_address) + ':8000'
    global NAMENODE_IP
    NAMENODE_IP = str(instance_check.public_ip_address)
    print('Waiting for Namenode to start...')
    time.sleep(60)

    print('Running New Node at:', RPC_NAMENODE_SERVER_URL)
    return instance_check


def terminate_ec2(instance_id):
    ec2 = boto3.resource('ec2')
    instance = ec2.Instance(instance_id)
    response = instance.terminate()
    print(response)


def start_nodes():
    new_namenode = create_ec2()
    global rpc_namenode
    global NAMENODE_IP

    ec2 = boto3.resource('ec2')
    for instance in ec2.instances.all():
        print('EC2:', instance.id, instance.state, instance.public_ip_address)

    RPC_NAMENODE_SERVER_URL = 'http://' + new_namenode.public_ip_address + ':8000'

    rpc_namenode = xmlrpclib.ServerProxy(RPC_NAMENODE_SERVER_URL)
    client.set_namenode(RPC_NAMENODE_SERVER_URL)
    print(rpc_namenode.myIp(NAMENODE_IP))
    print('Namenode Connected!', NAMENODE_IP)

    #terminate_ec2(new_namenode.id)


def createDataNodes(numDataNodes):
    dnIps = []
    i = 0
    while i < numDataNodes:
        #create datanodes
        ec2 = boto3.resource('ec2')
        instance_id = ''
        instance_check = None
        instance = ec2.create_instances(
        ImageId = 'ami-6be87e13',
        MinCount = 1,
        MaxCount = 1,
        InstanceType='t2.micro',
        #UserData='#!/bin/bash\r\npython /home/ec2-user/SUFS/Namenode/NamenodeServer.py'
        )
        instance_id = instance[0].id
        print('Created Datanode Server:', instance[0].id, instance[0].public_ip_address)
        instance_check = instance[0]
        print('Getting Public IP...')
        # Wait for server to
        while instance_check.public_ip_address == None:
            time.sleep(10)
            instance_check = ec2.Instance(instance_id)

        #store ips' in dnIps list
        print (str(instance_check.public_ip_address))
        dnIps.append(str(instance_check.public_ip_address))
        i = i + 1
    #outside of loop
    #wait for instances to boot up
    print ("waiting for datanodes to start")
    time.sleep(60)
    # go thru and send namenode ip and datanode ip
    for ip in dnIps:
        print (NAMENODE_IP)
        print (ip)
        #what is our datanode port?
        datanode = xmlrpclib.ServerProxy("http://" + str(ip) + ':' + '8888')
        #send namenode ip and the datanode ip
        datanode.receiveNNIp("http://" + NAMENODE_IP, "http://" + ip)
        print("started heartbeat on " + ip)
    return "yay! it worked"


# Function for handling connections. This will be used to create threads
def clientthread(conn):
    global rpc_namenode
    global RPC_NAMENODE_SERVER_URL

    # Sending message to connected client
    conn.send('Welcome to the SUFS MAIN Portal. Type command and hit enter and i will return it as a test\n') #send only takes string
    conn.send('Create File? type: cf \n' 'Read File? type: rf filename \n' 'Delete a file? type: df filename \n' 'Create directory? type: mkdir <path> <dir_name> \n'
              'Delete directory? type: rmdir <dir> \n' 'List contents of directory? type: ls <dir> \n'
              'List datanodes that store replicas of each block of a file? type: lsdnode \n' 'Press 0 to exit \n\n> ')

    # infinite loop so that function do not terminate and thread do not end.
    while True:

        # we need to parse data from the client, e.g. add a file
        data = conn.recv(1024)
        r = conn.recv(1024).decode()
        r = data
        cliInput = r.split()
        i = 0

        if cliInput[i] == 'cf':
            path = cliInput[i+1]
            file_name = cliInput[i+2]
            client.get_file_from_s3('testfile.txt')
            client.put_file_to_nn('/home/', 'testfile.txt')
            reply = 'Created a file!'
            # '''
            # #If we want to create bucket
            # myS3 = boto3.resource('s3')
            # myS3.create_bucket(Bucket='sufs-project', CreateBucketConfiguration={
            # 'LocationConstraint': 'us-west-2'})
            # '''
            #
            # # downloading file to current local directory from s3
            # """BUCKET_NAME = 'sufs-project'
            # KEY= 'part-r-00000'
            # s3 = boto3.resource('s3')
            # try:
            #     s3.Bucket(BUCKET_NAME).download_file(KEY, 'part-r-00000')
            # except botocore.exceptions.ClientError as e:
            #     if e.response['Error']['Code'] == "404":
            #         print("The object does not exist.")
            #     else:
            #         raise
            # """
            #
            # file_name = '/Users/justin/cs/cloud/input/testfile.txt'
            # input_file = open(file_name)
            # output_dir = "/Users/justin/cs/cloud/output"
            # input_file_size = os.path.getsize(file_name)
            # print("Getting filenameSize from Namenode...")
            # reply = rpc_namenode.write1(file_name, input_file_size)
            #
            # print("it worked!!!!!!!")
            #
            # # for debug
            # conn.send('file generated locally\n')
            #
            # # call splitFile() from block divider to divide blocks to pass to namenode to decide n datanodes to store blocks
            # bd = BlockDivider.BlockDivider()
            # bd.split_file(file_name, output_dir)
            # print (bd)
            # # splitFile() needs to be modified errno22 invalid mode ('w') line 39 in blockdivider.py
            #
            # '''
            # 1) send blocks and filename over to namenode
            # 2) wait for namenode to put file in to directory structure and picks N (replication factor) different data nodes to store each block
            # 3) wait for amenode to return list of blocks and datanodes and then pass the blocks to datanode to store
            # '''
            # reply = 'Completed task of creating new file in SUFS | next cmd: '

        elif cliInput[i] == 'rf filename':

            '''
            1) get info on where list of blocks are stored on which datanodes from the nameNode
            2) contact datanodes for the blocks
            3) read file based on blocks returned
            '''

            reply = 'access read file completed| next cmd: '

        elif cliInput[i] == 'df filename':
            '''
            1) get info on where list of blocks are stored on which datanodes from the nameNode
            2) contact datanodes for the blocks
            3) tell datanode to remove blocks and tell namenode to remove file in the directory and list of datanodes that holds the block
            '''
            reply = 'access delete file completed| next cmd: '

        elif cliInput[i] == 'hello':
            print("Connecting to Namenode...")
            reply = rpc_namenode.hello_world()

        #elif cliInput[i] == 'getfilesize':
        #    print("Getting filenameSize from Namenode...")
        #    reply = rpc_namenode.write1("testfile1.txt", 256)

        # elif cliInput[i] == 'runnamenode':
        #     print("Running Namenode")
        #     subprocess.call('ls', shell=True, cwd='/mnt/c/workspace/SUFS/Namenode')
        #     subprocess.call('python NamenodeServer.py', shell=True, cwd='/mnt/c/workspace/SUFS/Namenode')
        #
        #     # subprocess.call('cd Namenode', shell=True)
        #     # subprocess.call('ls', shell=True)
        #     reply = 'namenode started| next cmd: '
        #
        # elif cliInput[i] == 'rundatanode':
        #     print("Running datanode")
        #     subprocess.call('ls', shell=True, cwd='/mnt/c/workspace/SUFS/Namenode')
        #     subprocess.call('python DatanodeServer.py', shell=True, cwd='/mnt/c/workspace/SUFS/Datanode')
        #
        #     # subprocess.call('cd Namenode', shell=True)
        #     # subprocess.call('ls', shell=True)
        #     reply = 'namenode started| next cmd: '

        #######################
        # Directory Commands
        #######################

        # create a directory
        elif cliInput[i] == 'mkdir':
            path = ''
            dir = ''
            try:
                path = cliInput[i + 1]
                dir = cliInput[i + 2]
                rpc_namenode.mkdir(path, dir)
            except:
                dir = ''
            reply = 'create directory ' + path + dir

        # remove a directory
        elif cliInput[i] == 'rmdir':

            try:
                path = cliInput[i + 1]
                reply = rpc_namenode.deletedir(path)
            except:
                reply = 'could not delete path'

        # list directory contents
        elif cliInput[i] == 'ls':
            dir = ''
            try:
                dir = cliInput[i + 1]
                files = rpc_namenode.ls(dir)
                reply = 'Contents of ' + dir + ':\n'
                for f in files:
                    reply += '|_ ' + f + '\n'
            except:
                reply = 'failed list directory\n'

        #######################
        # Namenode Commands
        #######################

        # Create new Namenode
        elif cliInput[i] == 'createNN':
            try:
                start_nodes()
                reply = 'Started Namenode!'
            except botocore.exceptions.ClientError as e:
                reply = 'Could not create Namenode!\n' + e.message

        # Connect to existing Namenode
        elif cliInput[i] == 'connectNN':
            try:
                RPC_NAMENODE_SERVER_URL = cliInput[i+1]
                rpc_namenode = xmlrpclib.ServerProxy("http://" + str(RPC_NAMENODE_SERVER_URL) + ':8000')
                rpc_namenode.hello_world()
                client.set_namenode(RPC_NAMENODE_SERVER_URL)
                print('Connected to Namenode', RPC_NAMENODE_SERVER_URL)
                reply = 'Connected to Namenode!'
            except:
                reply = 'Could not connect to Namenode! '

        #######################
        # Datanode Commands
        #######################

        # Create new Datanode
        elif cliInput[i] == 'createDN':
            reply = createDataNodes(2)

        # Print list of Datanodes and Timestamps from Namenode
        elif cliInput[i] == 'printDN':
            reply = rpc_namenode.printDataNodes()

        #######################
        # Misc. Commands
        #######################

        # Exit
        elif cliInput[i] == '0':
            break
        else:
            reply = 'please type a correct command to the portal or 0 to exit: '

        conn.send(reply + '\n> ')
        i += 1

    # came out of loop
    conn.close()

    # for debug
    print('Thank you for using the SUFS')


# now keep talking with the client
while 1:
    # wait to accept a connection - blocking call
    conn, addr = s.accept()
    print('Connected with ' + addr[0] + ':' + str(addr[1]))

    # start new thread takes 1st argument as a function name to be run, second is the tuple of arguments to the function.
    start_new_thread(clientthread, (conn,))

s.close()
