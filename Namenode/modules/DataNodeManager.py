import boto3
import socket
import sys
import datetime


class DataNodeManager():
    def __init__(self):
        self.map = []
        self.nodeHeartBeat()

    def addNode(self, node):
        self.map[node] = datetime.datetime.now()

    def removeNode(self, node):
        del self.map[node]

    def heartBeat(self, conn):
        while True:
            #Receiving from client
            data = conn.recv(1024)
            reply = 'OK...' + data
            if not data:
                break

            conn.sendall(reply)
            if (node in map):
                self.map[data] = datetime.datetime.now()
            else:
                self.addNode(data)
        #came out of loop
        conn.close()

    def nodeHeartBeat(self):
        HOST = ''   # Symbolic name meaning all available interfaces
        PORT = 1234 # Arbitrary non-privileged port

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        print 'Socket created'

        #Bind socket to local host and port
        try:
            s.bind((HOST, PORT))
        except socket.error as msg:
            print 'Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
            sys.exit()

        print 'Socket bind complete'
        s.listen(10)
        while 1:
            #wait to accept a connection - blocking call
            conn, addr = s.accept()
            print 'Connected with ' + addr[0] + ':' + str(addr[1])

            #start new thread takes 1st argument as a function name to be run, second is the tuple of arguments to the function.
            start_new_thread(self.heartBeat ,(conn,))
        s.close()
