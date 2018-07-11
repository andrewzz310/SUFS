# Alex Lew, Hang Nguyen, Justin Shields, Andrew Zhu

SUFS is a replica of the HDFS (Hadoop Distributed File System) that runs inside AWS using EC2 instances to form the cluster.
We used Boto3 (AWS SDK for python) in order for our client (localhost) to connect to Namenode and Datanode EC2 instances. File storage was commenced in S3 buckets on AWS. Interactions and communication was done through RPC/Telnet/TCP. The SUFS distributed file system is resilient in storing and retrieving files, deletion of files, and includes fault tolerant datanodes based on replication, heart-beats, and block reports. For more information regarding HDFS:  https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html















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
Connect to an existing Namenode. ex: ```connectNN 127.0.0.1```
```
connectNN <host_ip>
```

Create Namenode ec2 instance.
```
createNN
```

Create Datanodes ec2 instance(s). Creates 4 datanodes by default. ex: ```createDN 6``` or ```createDN```
```
createDN <num_of_datanodes>
```

Connect to an existing Namenode. ex: ```connectNN 127.0.0.1```
```
connectNN <host_ip>
```

Add a file to SUFS. File must already exist on an S3 bucket. ex: ```cf /home/ sufs-shieldsj foo.txt```
```
cf <dir> <bucket_name> <file_name>
```

Remove a file to SUFS. ex: ```rm /home/ foo.txt```
```
rm <dir> <file_name>
```

Read (download) a file from SUFS. ex: ```read /home/ foo.txt```
```
read <dir> <file_name>
```

Create directory. ex: ```mkdir /home/ foo```
```
mkdir <path> <new_dir>
```

Delete directory. ex: ```rmdir /home/foo/```
```
rmdir <path>
```

List contents in a directory. ex: ```ls /home/foo/```
```
ls <path>
```

List the DataNodes that store replicas of each block of a file. ex: ```lsDN /home/ foo.txt```
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
