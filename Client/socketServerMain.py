"""
Simple socket server using multiple threads
"""

from __future__ import print_function
import socket
import sys
import boto3
import botocore  # for exception handling
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
PORT = 9999  # Arbitrary non-privileged port
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
        ImageId='ami-e602949e',
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
    client.set_namenode(NAMENODE_IP)
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
            ImageId='ami-ce7be8b6',
            MinCount=1,
            MaxCount=1,
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
    global NAMENODE_IP

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
            path = cliInput[i + 1]
            bucket_name = cliInput[i + 2]
            file_name = cliInput[i + 3]
            #client.save_file_from_s3(bucket_name, file_name)
            client.put_file_to_nn(path, bucket_name, file_name)
            reply = 'Created a file ' + file_name + '!'

        elif cliInput[i] == 'rm':
            '''
            1) get info on where list of blocks are stored on which datanodes from the nameNode
            2) contact datanodes for the blocks
            3) read file based on blocks returned
            '''
            path = cliInput[i + 1]
            file_name = cliInput[i + 2]
            print(client.delete_file(path, file_name))
            reply = 'Removed file ' + path + file_name

        elif cliInput[i] == "read":
            path = cliInput[i + 1]
            file_name = cliInput[i + 2]
            print(client.read_file(path, file_name))
            reply = 'Read file ' + path + file_name

        elif cliInput[i] == 'hello':
            print("Connecting to Namenode...")
            reply = rpc_namenode.hello_world()

        #######################
        # Directory Commands
        #######################

        # create a directory
        elif cliInput[i] == 'mkdir':
            path = ''
            dir = ''
            result = ''
            try:
                path = cliInput[i + 1]
                dir = cliInput[i + 2]
                result = rpc_namenode.mkdir(path, dir)
            except:
                dir = ''
            if result == 'Name of directory cannot any of reserved characters':
                reply = 'Name of directory cannot any of reserved characters'
            elif result == 'Fail to create a directory':
                reply = 'Fail to create a directory'
            elif result == 'Directory exists':
                reply = 'Directory exists'
            else:
                reply = 'create directory ' + path + dir

        # remove a directory
        elif cliInput[i] == 'rmdir':
            path = cliInput[i + 1]
            print(client.delete_dir(path))
            reply = 'Removed file ' + path

        # list directory contents
        elif cliInput[i] == 'ls':
            dir = ''
            try:
                dir = cliInput[i + 1]
                files = rpc_namenode.ls(dir)
                if files == 'No such directory':
                    reply = 'No such directory'
                else:
                    if not files:
                        reply = 'There is nothing in this directory'
                    else:
                        reply = 'Contents of ' + dir + ':\n'
                        for f in files:
                            reply += '|_ ' + f + '\n'
            except:
                reply = 'failed list directory\n'

        # List the DataNodes that store replicas of each block of a file
        elif cliInput[i] == 'lsDN':
            path = cliInput[i + 1]
            file_name = cliInput[i + 2]
            print(path + file_name)
            dict = rpc_namenode.lsDataNode(path + file_name)
            if not dict:
                reply = 'Cannot list the DataNodes that store replicas of each block of a file.  It may because of wrong filename or path'
            else:
                reply = 'Contents of ' + path + ' ' + file_name + ':\n'
                for blockID, listDN in sorted(dict.iteritems()):
                    strListDN = ""
                    for datanode in listDN:
                        strListDN += datanode
                        strListDN += ', '

                     #get rid of the last comma
                    strListDN = strListDN[:len(strListDN)-2]
                    reply += 'blockID = ' + blockID + '  ' + 'list of datanodes = [' + strListDN + ']\n'

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
                NAMENODE_IP = cliInput[i+1]
                print('Connecting to Namenode ' + NAMENODE_IP)
                RPC_NAMENODE_SERVER_URL = 'http://' + NAMENODE_IP + ':8000'

                rpc_namenode = xmlrpclib.ServerProxy(RPC_NAMENODE_SERVER_URL)
                print(rpc_namenode.myIp(NAMENODE_IP))
                rpc_namenode.hello_world()
                client.set_namenode(NAMENODE_IP)
                print('Connected to Namenode', RPC_NAMENODE_SERVER_URL)
                reply = 'Connected to Namenode!'
            except botocore.exceptions.ClientError as e:
                reply = 'Could not connect to Namenode!\n' + e.message

        elif cliInput[i] == 'printBR':
            try:
                RPC_NAMENODE_SERVER_URL = 'http://' + NAMENODE_IP + ':8000'
                rpc_namenode = xmlrpclib.ServerProxy(RPC_NAMENODE_SERVER_URL)
                br = rpc_namenode.getBlockReport()
                for ip in br.keys():
                    print (ip)
                    print (br[ip])
                    reply = "look at the client"
            except:
                print ("there was a problem")
                reply = "there was a problem"

        #######################
        # Datanode Commands
        #######################

        # Create new Datanode
        elif cliInput[i] == 'createDN':
            if len(cliInput) > 1:
                num_of_dn = cliInput[i + 1]
                reply = createDataNodes(int(num_of_dn))
            else:
                reply = createDataNodes(4)

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
