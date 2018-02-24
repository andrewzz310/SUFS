# SUFS - Seattle University File System


### Resources
1) This python SDK for AWS will be important to use for efficiency and practical purposes for EC2 namenode/datanode instances
https://github.com/boto/boto3#quick-start  (Boto3 is the Amazon Web Services (AWS) Software Development Kit (SDK) for Python, which
 allows Python developers to write software that makes use of services like Amazon S3 and Amazon EC2). E.g. writing a file to our namenode
from the client. 


## Upload a new file
```
data = open('test.jpg', 'rb')
s3.Bucket('my-bucket').put_object(Key='test.jpg', Body=data)
```

2) http://boto.readthedocs.io/en/latest/ref/ec2.html ---> This one explains how to directly connect to EC2 inside of our nodes....
(boto.ec2.connection)

## Useful Commands

#### Launching Namenode Server
From project root director
```
python Namenode/NamenodeServer.py
```

#### Launching Datanode Server
From project root director
```
python Datanode/DatanodeServer.py
```

#### Launching Client Server
From project root director
```
python Client/socketServerMain.py
```

#### Connecting to Client Server
```
telnet localhost 8888
```

Testing Namenode connection from telnet
```
hello
```
Testing Datanode connection from telnet
```
datanode
```