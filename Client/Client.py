import xmlrpclib
import os
import boto3
from modules import BlockDivider


class Client:

    def __init__(self):
        self.file_name = None
        self.path = None
        self.bucket_name = 'sufs-shieldsj'
        self.RPC_NAMENODE_SERVER_URL = ''
        self.rpc_namenode = None

    def set_namenode(self, url):
        self.RPC_NAMENODE_SERVER_URL = url
        self.rpc_namenode = xmlrpclib.ServerProxy("http://" + str(self.RPC_NAMENODE_SERVER_URL) + ':8000')

    # Main function
    def put_file_to_nn(self, path, file_name):
        self.path = path
        self.file_name = file_name
        self.save_file_from_s3(file_name)
        block_info = self.register_file_to_nn(file_name, os.path.getsize(file_name))

        print(block_info)
        # Split files
        #blocks = BlockDivider.BlockDivider.split_file(file_name, '')

        # Send each block to Datanode
        # for block in block_info:
        #     rpc_datanode = xmlrpclib.ServerProxy("http://" + str(block.datanode_ip) + ':8000')
        #
        #     with open(filename, "rb") as handle:
        #         return xmlrpclib.Binary(handle.read())
        #
        #     rpc_datanode.receiveBlock()

    def save_file_from_s3(self, file_name):
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=self.bucket_name, Key=file_name)

        temp_file = open(file_name, 'w+')
        temp_file.write(response['Body'].read())
        temp_file.close()
        print 'File Name:', file_name, 'File Size:', os.path.getsize(file_name)

    def show_all_s3_files(self):
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(self.bucket_name)
        result = list()

        for obj in bucket.objects.all():
            print(obj.key)
            result.append(obj.key)

        return result

    def register_file_to_nn(self, file_name, file_size):
        return self.rpc_namenode.putFile(file_name, file_size)
