"""
Block Divider
"""
import os


class BlockDivider:

    def __init__(self, blocksize):
        self.BLOCKSIZE = blocksize  # 4MB
        #self.BLOCKSIZE = 64000000  # in bytes aka 64MB

    def split_file(self, path, filename, out_path):
        """
        Splits a file into blocks and returns this location or newly created block files.
        :param filename: absolute path to file (ex:"/Users/justin/cs/cloud/input/testfile.txt")
        :param out_path: absolute path to file destination (ex:"/Users/justin/cs/cloud/output/")
        :return: a list of all block files created (ex: input='foo.txt', output=['foo.txt.part1', 'foo.txt.part2', ...]
        """

        output_list = []
        split_paths = os.path.split(filename)
        input_file_name = split_paths[1]
        print "file name: " + input_file_name

        path_hash = path.replace('/', '#')

        # open file
        input_file = open(filename)

        # get file size in bytes
        input_file_size = os.path.abspath.getsize(filename)
        print "file size: " + str(input_file_size)
        print "block size: " + str(self.BLOCKSIZE)

        # split file
        block_number = 1
        current_size = 0
        while current_size < input_file_size:
            output_data = input_file.read(self.BLOCKSIZE)
            output_file_name = out_path + path_hash + input_file_name + '.part' + str(block_number)
            output = open(output_file_name, 'w')
            output.write(output_data)
            output.close()

            output_list.append(output_file_name)
            block_number += 1
            current_size += self.BLOCKSIZE
            print "created file " + output_file_name

        # close file
        input_file.close()

        return output_list


"""
example of usage...
"""
#bd = BlockDivider()
#print bd.split_file("/Users/justin/cs/cloud/input/testfile.txt", "/Users/justin/cs/cloud/output/")
