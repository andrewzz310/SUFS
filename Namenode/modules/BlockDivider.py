"""
Block Divider
"""
import os


def splitFile(filename, parts):
    """

    :param filename:
    :param parts: number of file splits
    :return:
    """

    output_list = []
    outputBase = ""
    path = ""
    filesize = 0
    blocksize = 0

    split_paths = os.path.split(filename)

    outputBase = split_paths[1]
    path = split_paths[0]

    print "file name: " + outputBase

    # open file
    file = open(filename)

    filesize = os.path.getsize(filename)
    blocksize = int(filesize / parts)

    print "file size: " + str(filesize)
    print "block size: " + str(blocksize)

    at = 1
    for lines in range(0, parts):
        outputData = file.read(blocksize)
        outputName = path + "/" + outputBase + '.part' + str(at)
        output = open(outputName, 'w')
        output.write(outputData)
        output.close()

        output_list.append(outputName)
        at += 1
        print "created file " + outputName

    # close file
    file.close()


"""
for testing...
"""
splitFile("/Users/justin/cs/cloud/input/testfile.txt", 3)