'''
    Simple socket server using multiple threads
'''
from __future__ import print_function
import socket
import sys
import boto3
import botocore
import subprocess
import os
from thread import *
import modules.BlockDivider as BlockDivider
import modules.RPCClient as RPCClient


# For RPC client interactions
# TODO: make these dictionaries
rpc = RPCClient.RPCClient('http://localhost', 8000)
rpc_datanode = RPCClient.RPCClient('http://localhost', 8880)

HOST = ''   # Symbolic name meaning all available interfaces
PORT = 8888 # Arbitrary non-privileged port

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
print('Socket created')

#Bind socket to local host and port
try:
    s.bind((HOST, PORT))
except socket.error as msg:
    print('Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1])
    sys.exit()

print('Socket bind complete')

#Start listening on socket
s.listen(10)
print('Server now listening')




#Function for handling connections. This will be used to create threads
def clientthread(conn):
    #Sending message to connected client
    conn.send('Welcome to the SUFS MAIN Portal. Type command and hit enter and i will return it as a test\n') #send only takes string
    conn.send('Create File? type: cf \n' 'Read File? type: rf filename \n' 'Delete a file? type: df filename \n' 'Create directory? type: cdir \n'
              'Delete directory? type: deldir \n' 'List contents of directory? type: lsdir \n'
              'List datanodes that store replicas of each block of a file? type: lsdnode \n' 'Press 0 to exit \n')
    #infinite loop so that function do not terminate and thread do not end.
    while True:


        #we need to parse data from the client, e.g. add a file
        data = conn.recv(1024)
        r = conn.recv(1024).decode()
        r = data
        cliInput = r.split()
        i = 0
        print(cliInput[i])

        if cliInput[i] == 'cf':
            '''
            #If we want to create bucket
            myS3 = boto3.resource('s3')
            myS3.create_bucket(Bucket='sufs-project', CreateBucketConfiguration={
            'LocationConstraint': 'us-west-2'})
            '''

            #downloading file to current local directory from s3
            """BUCKET_NAME = 'sufs-project'
            KEY= 'part-r-00000'
            s3 = boto3.resource('s3')
            try:
                s3.Bucket(BUCKET_NAME).download_file(KEY, 'part-r-00000')
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == "404":
                    print("The object does not exist.")
                else:
                    raise
            """

            file_name = '/Users/justin/cs/cloud/input/testfile.txt'
            input_file = open(file_name)
            output_dir = "/Users/justin/cs/cloud/output"
            input_file_size = os.path.getsize(file_name)
            print("Getting filenameSize from Namenode...")
            reply = rpc.write1(file_name, input_file_size)

            print("it worked!!!!!!!")

            #for debug
            conn.send('file generated locally\n')
            ##call splitFile() from block divider to divide blocks to pass to namenode to decide n datanodes to store blocks
            bd = BlockDivider.BlockDivider()
            bd.split_file(file_name, output_dir)
            print (bd)
            #splitFile() needs to be modified errno22 invalid mode ('w') line 39 in blockdivider.py

            '''
            1) send blocks and filename over to namenode
            2) wait for namenode to put file in to directory structure and picks N (replication factor) different data nodes to store each block
            3) wait for amenode to return list of blocks and datanodes and then pass the blocks to datanode to store
            '''
            reply = 'Completed task of creating new file in SUFS | next cmd: '


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

        elif cliInput[i] == 'cdir':
            reply = 'access create directory s3| next cmd: '

        elif cliInput[i] == 'deldir':
            reply = 'access delete directory s3| next cmd: '

        elif cliInput[i] == 'lsdir':
            reply = 'access list contents of directory s3| next cmd: '

            #need to figure out how to pass to client
            s3 = boto3.resource('s3')
            for bucket in s3.buckets.all():
                print(bucket.name)

        elif cliInput[i] == 'lsdnode':
            reply = 'access list datanotes that store replicas of each block of file s3| next cmd: '

        elif cliInput[i] == 'hello':
            print("Connecting to Namenode...")
            reply = rpc.hello_world()

        elif cliInput[i] == 'datanode':
            print("Connecting to Datanode...")
            reply = rpc_datanode.hello_world()

        elif cliInput[i] == 'getfilesize':
            print("Getting filenameSize from Namenode...")
            reply = rpc.write1("testfile1.txt", 256)

        elif cliInput[i] == 'runnamenode':
            print("Running Namenode")
            subprocess.call('ls', shell=True, cwd='/mnt/c/workspace/SUFS/Namenode')
            subprocess.call('python NamenodeServer.py', shell=True, cwd='/mnt/c/workspace/SUFS/Namenode')

            # subprocess.call('cd Namenode', shell=True)
            # subprocess.call('ls', shell=True)
            reply = 'namenode started| next cmd: '

        elif cliInput[i] == 'rundatanode':
            print("Running datanode")
            subprocess.call('ls', shell=True, cwd='/mnt/c/workspace/SUFS/Namenode')
            subprocess.call('python DatanodeServer.py', shell=True, cwd='/mnt/c/workspace/SUFS/Datanode')

            # subprocess.call('cd Namenode', shell=True)
            # subprocess.call('ls', shell=True)
            reply = 'namenode started| next cmd: '

        elif cliInput[i] == 'createdir':
            dir = ''
            try:
                dir = cliInput[i+1]
            except:
                dir = ''
            # create a directory
            reply = 'create directory ' + dir

        elif cliInput[i] == 'deletedir':
            # remove a directory
            reply = 'remove directory'

        elif cliInput[i] == '0':
            break
        else:
            reply = 'please type a correct command to the portal or 0 to exit: '

        conn.sendall(reply)
        i+=1


    #came out of loop
    conn.close()

    #for debug
    print('Thank you for using the SUFS')

#now keep talking with the client
while 1:
    #wait to accept a connection - blocking call
    conn, addr = s.accept()
    print('Connected with ' + addr[0] + ':' + str(addr[1]))

    #start new thread takes 1st argument as a function name to be run, second is the tuple of arguments to the function.
    start_new_thread(clientthread ,(conn,))

s.close()
