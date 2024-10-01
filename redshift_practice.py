import boto3

import configparser

config = configparser.ConfigParser()

config.read_file(open('dwh.cfg'))

KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')


DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")
DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
DWH_DB = config.get("DWH", "DWH_DB")
DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")
# DWH_PORT               = config.get("DWH","DWH_PORT")
DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")


# def get_aws_access_key(fullFilePath):
#     with open(fullFilePath) as ffp:
#         ID, KEY = tuple(ffp.readlines())[-1].split(',')
#     return ID, KEY


# ID, KEY = get_aws_access_key('dwhadmin_accessKeys.csv')


redshift = boto3.client(
    'redshift',
    region_name="us-east-2",
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET
)

respone = redshift.create_cluster(
    ClusterType=DWH_CLUSTER_TYPE,
    NodeType=DWH_NODE_TYPE,
    NumberOfNodes=int(DWH_NUM_NODES),

    DBName=DWH_DB,
    ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
    MasterUsername=DWH_DB_USER,
    MasterUserPassword=DWH_DB_PASSWORD,

    IamRoles=[DWH_IAM_ROLE_NAME]
)

print(redshift)
print(respone)

exit()

dwhrole = iam.create_role(
    Path='/',
    RoleName=DWH_IAM_ROLE_NAME,
    Description='Allows redshift clusters to call aws on our behalf',
    AssumeRolePolicyDocument=json.dumps({
        'Statement': [{'Action': 'sts:AssumeRole',
                      'Effect': 'Allow',
                       'Principal': {
                           'Service': 'redshift.amazonaws.com'
                       }
                       }],
        'Version': '2024-09-21'
    })
)

iam.attach_role_policy(
    RoleName=DWH_IAM_ROLE_NAME,
    PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
)['ResponseMetadata'['HTTPStatusCode']]


vpc = ec2.Vpc(id=myClusterProps['VpcId'])

defaultSg = list(vpc.security_groups.all())[0]
defaultSg.authorize_ingress(
    GroupName=defaultSg.group_name,
    CidrIp='0.0.0.0/0',
    IpProtocol='TCP',

)
