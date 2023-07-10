import boto3
import logging

def role_arn_to_session(session=None, account_id=None):
    if session:
        if session.client('sts').get_caller_identity()["Account"] == account_id:
            return session
        else:
            client = session.client("sts")
    else:
        client = boto3.client("sts")
    response = client.assume_role(
        RoleArn=f"arn:aws:iam::{account_id}:role/AdministratorReadWriteAccess",
        RoleSessionName=f"aws-{account_id}-role",
    )
    return boto3.Session(
        aws_access_key_id=response["Credentials"]["AccessKeyId"],
        aws_secret_access_key=response["Credentials"]["SecretAccessKey"],
        aws_session_token=response["Credentials"]["SessionToken"],
)

### S3 Check

def get_bucket_config(session,bucket_name):
    response = session.get_bucket_encryption(Bucket=bucket_name)
    encryption = response['ServerSideEncryptionConfiguration']
    response = session.get_public_access_block(Bucket=bucket_name)
    public_access = response['PublicAccessBlockConfiguration']
    ##get bucket policy
    try:
        bucket_policy=session.get_bucket_policy(Bucket=bucket_name)['Policy']
    except Exception as e:
        logging.warning(e)
        bucket_policy=None
    ## get bucket ownership controls
    try:
        response = session.get_bucket_ownership_controls(Bucket=bucket_name)
        ownership = response['OwnershipControls']['Rules'][0]['ObjectOwnership']
    except Exception as e:
        logging.warning(e)
        ownership='ObjectWriter'
    ### update bucket cors
    try:
        response = session.get_bucket_cors(Bucket=bucket_name)
        cors = response['CORSRules']
    except Exception as e:
        logging.warning(e)
        cors=None
    try:
        session.put_bucket_versioning(
            Bucket=bucket_name,
            VersioningConfiguration={
                'Status': 'Enabled'
            },
        )
    except:
        pass

    try:
        response = session.get_bucket_tagging(Bucket=bucket_name)
        tag = response['TagSet']
    except Exception as e:
        logging.warning(e)
        tag=None
    return bucket_policy, cors, encryption, ownership, tag, public_access

## create s3 cross_account
def create_bucket(session,account_id, bucket_name, region, **config):
    logging.info(f'Creating Bucket  on account {account_id} with name {bucket_name}')
    try:
        session.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={
                    'LocationConstraint': region,
                },
                ObjectLockEnabledForBucket=False,
                ObjectOwnership=config["ownership"]
            )
    except Exception as e:
        logging.warning(e)
        
    if config["cors"]:
        session.client.put_bucket_cors(
            Bucket='string',
            CORSConfiguration={
                'CORSRules': config[cors]
            }
        )
    
    if config["tag"]:
        session.put_bucket_tagging(
            Bucket=bucket_name,
            Tagging={
                'TagSet': config["tag"]
            }
        )
        logging.info(f'Update bucket policy  on account {account_id} with name {bucket_name}')

    ## update bucket policy
    session.put_bucket_policy(
        Bucket=bucket_name,
        Policy=config["bucket_policy"]
    )

        ## update public access block
    logging.info(f'Update Bucket public access on account {account_id} with name {bucket_name}')
    session.put_public_access_block(
        Bucket=bucket_name,
        PublicAccessBlockConfiguration=config["public_access"]
        )

    logging.info(f'Update Bucket versioning on account {account_id} with name {bucket_name}')
    ## Enable Versioning
    try:
        session.put_bucket_versioning(
                Bucket=bucket_name,
                VersioningConfiguration={
                    'Status': 'Enabled'
                },
            )
    except Exception as e:
        logging.warning(e)

    logging.info(f'Update Bucket encryption on account {account_id} with name {bucket_name}')
    if config["encryption"]['Rules'][0]['ApplyServerSideEncryptionByDefault']['SSEAlgorithm'] == 'AES256':
        session.put_bucket_encryption(
        Bucket=bucket_name,
        ServerSideEncryptionConfiguration=config["encryption"]
        )

def create_replication(session=None,
                        src_bucket_name=None,
                        dst_account_id=None,
                        dst_bucket_name=None,
                        replication_role_arn=None):
    replication_config={
                'Role': replication_role_arn,
                'Rules': [{'DeleteMarkerReplication': {'Status': 'Enabled'},
                'Destination': {'AccessControlTranslation': {'Owner': 'Destination'},
                    'Account': f'{dst_account_id}',
                    'Bucket': f'arn:aws:s3:::{dst_bucket_name}',
                    'Metrics': {'EventThreshold': {'Minutes': 15},
                                'Status': 'Enabled'},
                    'ReplicationTime': {'Status': 'Enabled',
                                        'Time': {'Minutes': 15}}},
                'Filter': {},
                'ID': f'to-{dst_account_id}-{dst_bucket_name}',
                'Priority': 10,
                'SourceSelectionCriteria': {'ReplicaModifications': {'Status': 'Enabled'}},
                'Status': 'Enabled'}]
                }
            
    logging.info(f"Create replication config on {src_bucket_name}")
    try:
        session.put_bucket_replication(
            Bucket=src_bucket_name,
            ReplicationConfiguration=replication_config
        )
    except Exception as e:
        logging.warning(e)


def create_job(session=None, **config):
    response = session.create_job(**config)
    return response


