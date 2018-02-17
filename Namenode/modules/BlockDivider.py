"""
Block Divider
"""
import os


def splitFile(filename):
    """

    :param filename:
    :param parts: number of file splits
    :return:
    """
    BLOCKSIZE = 256

    output_list = []
    outputBase = ""
    path = ""
    filesize = 0

    split_paths = os.path.split(filename)

    outputBase = split_paths[1]
    path = split_paths[0]

    print "file name: " + outputBase

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
        outputName = path + "/" + outputBase + '.part' + str(at)
        output = open(outputName, 'w')
        output.write(outputData)
        output.close()

        output_list.append(outputName)
        at += 1
        currentsize += BLOCKSIZE
        print "created file " + outputName

    # close file
    file.close()


"""
for testing...
"""
#splitFile("/Users/justin/cs/cloud/input/testfile.txt")