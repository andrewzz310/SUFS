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
        self.alive = {}
        self.REPLICATION = 3

    def set_namenode(self, url):
        self.RPC_NAMENODE_SERVER_URL = url
        self.rpc_namenode = xmlrpclib.ServerProxy("http://" + str(self.RPC_NAMENODE_SERVER_URL) + ':8000')

    # Main function
    def put_file_to_nn(self, path, file_name):
        self.path = path
        self.file_name = file_name
        self.save_file_from_s3(self.file_name)
        block_info = self.register_file_to_nn(self.path, self.file_name, os.path.getsize(self.file_name))
        block_divider = BlockDivider.BlockDivider()

        print('block info:', block_info)

        # Split files
        blocks = block_divider.split_file(path, self.file_name, '')

        # Send each block to Datanode
        for block in block_info:
            print 'Connected to Datanode ' + str(block[1]) + ' and ' + block[0]
            rpc_datanode = xmlrpclib.ServerProxy(str(block[1]) + ':8888')

            # TODO: remove this later...
            #rpc_datanode.receiveNNIp(self.RPC_NAMENODE_SERVER_URL, '127.0.0.1')

            with open(block[0], "rb") as handle:
                obj = xmlrpclib.Binary(handle.read())

            print(rpc_datanode.receiveBlock(block[0], obj))

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

    def register_file_to_nn(self, path, file_name, file_size):
        return self.rpc_namenode.putFile(path, file_name, file_size)

    def delete_file(self, path, file_name):
        datanode_list = self.rpc_namenode.deleteFile(path, file_name)
        print(datanode_list)
        if not datanode_list:
            return 'Block does not exists...'
        else:
            self.remove_files_from_datanodes(datanode_list)

        return 'Removed blocks!'



    def remove_files_from_datanodes(self, dn_list):
        # connect to each datanode and remove block
        for block in dn_list:
            for dn in dn_list[block]:
                print('dn:', dn)
                dn_rpc = xmlrpclib.ServerProxy(dn + ':8888')
                dn_rpc.removeBlock(block)
                print('Deleted Block ' + block + ' from ' + dn)
        return True



    def delete_dir(self, dir_path):
        datanode_list = self.rpc_namenode.deleteDirectory(dir_path)
        print(datanode_list)
        if not datanode_list:
            return 'Block does not exists...'
        else:
            self.remove_files_from_datanodes(datanode_list)

        return 'Removed blocks!'