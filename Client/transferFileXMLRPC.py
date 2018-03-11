from SimpleXMLRPCServer import SimpleXMLRPCServer
import xmlrpclib

# client side is function writeBlocks in DataNode.py
class sendServer:
    def __init__(self, hostIP):
        self.hostIP = hostIP
        self.startServer()



    # filename is defined by NameNode
    def sendFileToDN(filename):
        with open(filename, "rb") as handle:
            return xmlrpclib.Binary(handle.read())



    def startServer(self):
    # need Public DNS (IPv4)
        server = SimpleXMLRPCServer((self.hostIP, 5000))
        print "Listening on port 5000..."
        server.register_function(self.sendFileToDN, 'sendFileToDN')

        server.serve_forever()
