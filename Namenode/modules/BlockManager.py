# import boto3
# import socket
# import sys
# import time
# from thread import *
#
#
# class BlockManager():
#     def __init__(self):
#         self.blocks = {}
#         start_new_thread(self.nodeHeartBeat, ())
#
#     def heartBeat(self, conn, server):
#         #Receiving from client
#         #does this actually receive all data?
#         data = conn.recv(1024)
#         reply = 'OK... ' + data
#         #if not data:
#             #break
#         conn.sendall(reply)
#         self.blocks[server] = data
#         #came out of loop
#         conn.close()
#
#     def blockReportHeartBeat(self):
#         HOST = ''   # Symbolic name meaning all available interfaces
#         PORT = 3333 # Arbitrary non-privileged port
#
#         s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#         print 'Socket created'
#
#         #Bind socket to local host and port
#         try:
#             s.bind((HOST, PORT))
#         except socket.error as msg:
#             print 'Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
#             sys.exit()
#
#         print 'Socket bind complete'
#         s.listen(10)
#         while 1:
#             #wait to accept a connection - blocking call
#             conn, addr = s.accept()
#             print 'Connected with ' + addr[0] + ':' + str(addr[1])
#
#             #start new thread takes 1st argument as a function name to be run, second is the tuple of arguments to the function.
#             start_new_thread(self.heartBeat , (conn, str(addr[1])))
#         s.close()

import time
#import modules.RPCClient as RPCClient
from SimpleXMLRPCServer import SimpleXMLRPCServer
from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler

# Restrict to a particular path.
class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

# Create server
server = SimpleXMLRPCServer(("localhost", 5000),
                            requestHandler=RequestHandler)
server.register_introspection_functions()



#class BlockManager():
#    def __init__(self):
#         self.blocks = {}

# file -> blockID, blockID ...
def get_blockID():
    return [1, 2, 3]

# blockID -> <DN, DN}
def get_DataNodeNumber():
    return ["get_DataNodeNumber", "test"]

server.register_function(get_blockID)
server.register_function(get_DataNodeNumber)

# Run the server's main loop
server.serve_forever()