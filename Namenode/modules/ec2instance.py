import boto3
import sys

def createEC2():
    ec2 = boto3.resource('ec2')
    for instance in ec2.instances.all():
        print instance.id, instance.state, instance.public_ip_address


def listEC2():
    ec2 = boto3.resource('ec2')
    instance = ec2.create_instances(
        # need to REPLACE SOONNNNNNNNNNNNNNNN
        ImageId='ami-9dd860e5',
        MinCount=1,
        MaxCount=1,
        InstanceType='t2.micro')
    print instance[0].id, instance[0].public_ip_address


def terminateEC2(instance_id):
    ec2 = boto3.resource('ec2')
    #for instance_id in sys.argv[1:]:
    instance = ec2.Instance(instance_id)
    response = instance.terminate()
    print response
