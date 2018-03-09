from SimpleXMLRPCServer import SimpleXMLRPCServer
import xmlrpclib

#filename is defined by NameNode
def sendFileToDN(filename):
     with open(filename, "rb") as handle:
         return xmlrpclib.Binary(handle.read())


server = SimpleXMLRPCServer(("localhost", 5000))
print "Listening on port 5000..."
server.register_function(sendFileToDN, 'sendFileToDN')

server.serve_forever()