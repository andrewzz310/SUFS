'''
    Simple socket server using multiple threads
'''
from __future__ import print_function
import os
import socket
import sys
import boto3
import botocore
from thread import *
from main import *




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
    conn.send('Create File? type: cf \n' 'Read File? type: rf \n' 'Delete a file? type: df  \n' 'Create directory? type: cdir \n'
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
            BUCKET_NAME = 'sufs-project'
            KEY= 'part-r-00000'
            s3 = boto3.resource('s3')
            try:
                s3.Bucket(BUCKET_NAME).download_file(KEY, 'part-r-00000')
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == "404":
                    print("The object does not exist.")
                else:
                    raise
            ##call splitFile() from block divider to divide blocks to pass to namenode to decide n datanodes to store blocks



            reply = 'Created new file in SUFS | next cmd: '


        elif cliInput[i] == 'rf':

            reply = 'access read file s3| next cmd: '

        elif cliInput[i] == 'df':
            reply = 'access delete file s3| next cmd: '

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
