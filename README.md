# SUFS - Seattle University File System

## Useful Commands

#### Launching Namenode Server
From SUFS directory
```
python Namenode/NamenodeServer.py
```

#### Launching Datanode Server
From SUFS directory
```
python Datanode/DatanodeServer.py
```

#### Launching Client Server
From SUFS directory
```
python Client/socketServerMain.py
```

#### Connecting to Client Server
```
telnet <host_name> 9999
```

## Telnet Commands
Connect to an existing Namenode. example: connectNN 127.0.0.1
```
connectNN <host_ip>
```

Create Namenode ec2 instance.
```
createNN
```

Create Datanode(s) ec2 instance(s).
```
createDN
```

Connect to an existing Namenode. example: connectNN 127.0.0.1
```
connectNN <host_ip>
```

Add a file to SUFU. File must already exist on an S3 bucket. example: cf /home/ foo.txt
```
cf <dir> <file_name>
```

Remove a file to SUFU. example: rm /home/ foo.txt
```
rm <dir> <file_name>
```

Read (download) a file from SUFU. example: read /home/ foo.txt
```
read <dir> <file_name>
```

Create directory. example: mkdir /home/ foo
```
mkdir <path> <new_dir>
```

Delete directory. example: rmdir /home/foo/
```
rmdir <path>
```

List contents in a directory. example: ls /home/foo/
```
ls <path>
```

List the DataNodes that store replicas of each block of a file. lsDN /home/ foo.txt
```
lsDN <path> <file_name>
```

Show block report.
```
printBR 
```

Print list of Datanodes and Timestamps from Namenode
```
printDN
```

Exit telnet connection
```
0
```

## Miscellaneous Notes

Script to run NamenodeServer on ec2 boot-up.
```
sudo nano /etc/rc.local
python /home/ec2-user/SUFS/Namenode/NamenodeServer.py
```

Script to run DamenodeServer on ec2 boot-up.
```
sudo nano /etc/rc.local
python /home/ec2-user/SUFS/Damenode/DamenodeServer.py
```

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

#### Create ec2 instance
```
#!/usr/bin/env python
import boto3
ec2 = boto3.resource('ec2')
instance = ec2.create_instances(
    ImageId='ami-9dd860e5',
    MinCount=1,
    MaxCount=1,
    InstanceType='t2.micro')
print instance[0].id, instance[0].public_ip_address
````

#### List ec2 instance
````
import boto3
ec2 = boto3.resource('ec2')
for instance in ec2.instances.all():
    print instance.id, instance.state, instance.public_ip_address
````
