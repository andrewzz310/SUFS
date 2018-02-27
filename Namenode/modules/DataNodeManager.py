import boto3
import socket
import sys
import time
from thread import *
import DataNodeRPCClient as RPCClient
import xmlrpclib


class DataNodeManager():
    def __init__(self):
        self.alive = {}
        self.files = {} # key = filename, value = [(part1, datanode1), (part2, datanode2), ...]
        self.rpc = RPCClient.RPCClient('http://localhost', 8000)
        start_new_thread(self.nodeHeartBeat, ())

    def addNode(self, node):
        self.alive[node] = time.time()

    def removeNode(self, node):
        del self.alive[node]

    def getDataNodes(self):
        return self.alive

    def checkTimes(self):
        for key in self.alive:
            diff = time.time() - self.alive[key]
            if (diff > 10):
                removeNode(key)

    def heartBeat(self):
        for ip in self.alive.keys():
            self.rpc = RPCClient.RPCClient(ip, 8000)
            #if rpc server no response what happens
            try:
                reply = rpc.ekg()
                if (reply):
                    self.alive[ip] = time.time()
            except xmlrpclib.ProtocolError as err:
                print "A protocol error occurred"
                print "URL: %s" % err.url
                print "HTTP/HTTPS headers: %s" % err.headers
                print "Error code: %d" % err.errcode
                print "Error message: %s" % err.errmsg

    def nodeHeartBeat(self):
        while 1:
            time.sleep(30) #sleep for 30 seconds
            heartBeat(self)
            checkTimes(self)
