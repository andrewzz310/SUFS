from SimpleXMLRPCServer import SimpleXMLRPCServer
import xmlrpclib

# client side is function writeBlocks in DataNode.py

# filename is defined by NameNode
def sendFileToDN(filename):
    with open(filename, "rb") as handle:
        return xmlrpclib.Binary(handle.read())

# need Public DNS (IPv4)
server = SimpleXMLRPCServer((IPv4, 5000))
print "Listening on port 5000..."
server.register_function(sendFileToDN, 'sendFileToDN')

server.serve_forever()
