"""
Block Divider
"""
import os


def splitFile(filename, outpath):
    """
    Splits a file into blocks and returns this location or newly created block files.
    :param filename: absolute path to file (ie:"/Users/justin/cs/cloud/input/testfile.txt")
    :param outpath: absolute path to file destination (ie:"/Users/justin/cs/cloud/output/")
    :return: a list of all block files created
    """
    BLOCKSIZE = 256

    output_list = []
    output_base = ""
    path = ""
    filesize = 0

    split_paths = os.path.split(filename)

    output_base = split_paths[1]
    path = split_paths[0]

    print "file name: " + output_base

    # open file
    file = open(filename)

    # get file size in bytes
    filesize = os.path.getsize(filename)

    print "file size: " + str(filesize)
    print "block size: " + str(BLOCKSIZE)

    at = 1
    currentsize = 0
    while currentsize < filesize:
        outputData = file.read(BLOCKSIZE)
        outputName = outpath + output_base + '.part' + str(at)
        output = open(outputName, 'w')
        output.write(outputData)
        output.close()

        output_list.append(outputName)
        at += 1
        currentsize += BLOCKSIZE
        print "created file " + outputName

    # close file
    file.close()

    return output_list

"""
for testing...
"""
print splitFile("/Users/justin/cs/cloud/input/testfile.txt", "/Users/justin/cs/cloud/output/")
