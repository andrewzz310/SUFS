'''
    Simple socket server using multiple threads
'''

import socket
import sys
import boto3
from thread import *
from main import *



HOST = ''   # Symbolic name meaning all available interfaces
PORT = 8888 # Arbitrary non-privileged port

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
print 'Socket created'

#Bind socket to local host and port
try:
    s.bind((HOST, PORT))
except socket.error as msg:
    print 'Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
    sys.exit()

print 'Socket bind complete'

#Start listening on socket
s.listen(10)
print 'Server now listening'




#Function for handling connections. This will be used to create threads
def clientthread(conn):
    #Sending message to connected client
    conn.send('Welcome to the SUFS MAIN Portal. Type command and hit enter and i will return it as a test\n') #send only takes string
    conn.send('Create File? type: cf \n' 'Read File? type: rf \n' 'Delete a file? type: df  \n' 'Create directory? type: cdir \n'
              'Delete directory? type: deldir \n' 'List contents of directory? type: lsdir \n'
              'List datanodes that store replicas of each block of a file? type: lsdnode \n')
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
            reply = 'access create file S3 ' + cliInput[0]

        elif cliInput[i] == 'rf':
            reply = 'access read file s3 ' + cliInput[0]

        elif cliInput[i] == 'df':
            reply = 'access delete file s3 ' + cliInput[0]

        elif cliInput[i] == 'cdir':
            reply = 'access create directory s3 ' + cliInput[0]

        elif cliInput[i] == 'deldir':
            reply = 'access delete directory s3 ' + cliInput[0]

        elif cliInput[i] == 'lsdir':
            reply = 'access list contents of directory s3 ' + cliInput[0]

        elif cliInput[i] == 'lsdnode':
            reply = 'access list datanotes that store replicas of each block of file s3 ' + cliInput[0]

        else:
            reply = 'please type a correct command to the portal'



        conn.sendall(reply)
        i+=1

        '''
        #Receiving from client
        data = conn.recv(1024)
        reply = 'OK...' + data
        if not data:
            break
        '''

    #came out of loop
    conn.close()

    #for debug
    print 'out of loop'

#now keep talking with the client
while 1:
    #wait to accept a connection - blocking call
    conn, addr = s.accept()
    print 'Connected with ' + addr[0] + ':' + str(addr[1])

    #start new thread takes 1st argument as a function name to be run, second is the tuple of arguments to the function.
    start_new_thread(clientthread ,(conn,))

s.close()
