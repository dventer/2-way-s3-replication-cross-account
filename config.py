from s3 import *
import csv
import logging
import sys
import json

role_name_format="arn:aws:iam::{}:role/{}"
src_role_replication_name=""
dst_role_replication_name=""
batch_replication_role_name=""
src_account_id = ""
src_region="ap-southeast-1"
dst_region="ap-southeast-3"
csv_file_name="bucket.csv"

logging.basicConfig(level=logging.INFO,
                    format="%(levelname)s | %(asctime)s | %(message)s",
                    datefmt="%Y-%m-%dT%H:%M:%SZ",
                    handlers=[
                    logging.FileHandler("bucket.log"),
                    logging.StreamHandler(sys.stdout)
    ])


data_result = []
src_assume_role=role_arn_to_session(account_id=src_account_id)
src_session = src_assume_role.client('s3', region_name=src_region)
src_s3_control = src_assume_role.client('s3control', region_name=src_region)
with open(csv_file_name) as csvfile:
    data = csv.DictReader(csvfile)
    for row in data:
        src_bucket_name= row['src_bucket_name']
        dst_bucket_name= row['dst_bucket_name']
        dst_account_id=row['dst_account_id']
        bucket_policy_template={
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "AWS": f"arn:aws:iam::{src_account_id}:root"
                },
                "Action": [
                    "s3:ReplicateObject",
                    "s3:ReplicateDelete",
                    "s3:ReplicateTags",
                    "s3:ObjectOwnerOverrideToBucketOwner"
                ],
                "Resource": f"arn:aws:s3:::{dst_bucket_name}/*"
            },
            {
                "Effect": "Allow",
                "Principal": {
                    "AWS": f"arn:aws:iam::{src_account_id}:root"
                },
                "Action": [
                    "s3:List*",
                    "s3:GetBucketVersioning",
                    "s3:PutBucketVersioning"
                ],
                "Resource": f"arn:aws:s3:::{dst_bucket_name}"
            }
        ]
    }
        ## Assume role to destination account
        dst_assume_role = role_arn_to_session(session=src_assume_role, account_id=dst_account_id)
        dst_session = dst_assume_role.client('s3', region_name=dst_region)

        ### get the configuration from source bucket
        bucket_policy, cors, encryption, ownership, tag, public_access = get_bucket_config(src_session, src_bucket_name)

        ## create bucket policy template for destination bucket
        if bucket_policy:
            temp_bucket_policy = bucket_policy.replace(dst_account_id, '@@@@@@@@').replace(src_bucket_name, dst_bucket_name)
            sanitize_bucket_policy = temp_bucket_policy.replace(src_account_id,dst_account_id).replace('@@@@@@@@', src_account_id)
            dst_bucket_policy = json.loads(sanitize_bucket_policy)
            for object in bucket_policy_template['Statement']:
                dst_bucket_policy['Statement'].append(object)
        else:
            dst_bucket_policy=bucket_policy_template

        
        ## create new bucket on destination acccount
        create_bucket(dst_session,dst_account_id, dst_bucket_name,dst_region,
                    cors=cors,
                    encryption=encryption,
                    ownership=ownership,
                    tag=tag,
                    public_access=public_access,
                    bucket_policy=json.dumps(dst_bucket_policy)
                    )
        
        ## generate replication Role arn for each account
        src_replication_role_arn=role_name_format.format(src_account_id, src_role_replication_name)
        dst_replication_role_arn=role_name_format.format(dst_account_id, dst_role_replication_name)

        #create replication on source bucket
        create_replication(session=src_session, dst_bucket_name=dst_bucket_name, dst_account_id=dst_account_id,
                            src_bucket_name=src_bucket_name, replication_role_arn=src_replication_role_arn)
        
        #create replication on destination bucket
        if not ownership == 'ObjectWriter':
            create_replication(session=dst_session, dst_bucket_name=src_bucket_name, dst_account_id=src_account_id,
                            src_bucket_name=dst_bucket_name, replication_role_arn=dst_replication_role_arn)

        ## create job
        logging.info(f"create replicate job on account {src_account_id}")
    #     job_id=create_job(session=src_s3_control,
    #         AccountId=src_account_id,
    #         ConfirmationRequired=False,
    #         Operation={
    #             'S3ReplicateObject': {}
    #         },
    #         Report={
    #             'Bucket': f'arn:aws:s3:::finaccel-aws-logs/s3-replications/{src_bucket_name}',
    #             'Enabled': True,
    #             'ReportScope': 'FailedTasksOnly'
    #         },
    #         Description=f'replicate from {src_bucket_name} to {dst_bucket_name}',
    #         Priority=10,
    #         RoleArn=role_name_format.format(src_account_id, "S3ReplicationBatchJobRole"),
    #         # Tags=[
    #         #     {
    #         #         'Key': 'BUCKET_NAME',
    #         #         'Value': f'{src_bucket_name}'
    #         #     },
    #         # ],
    #         ManifestGenerator={
    #             'S3JobManifestGenerator': {
    #                 'ExpectedBucketOwner': src_account_id,
    #                 'SourceBucket': f'arn:aws:s3:::{src_bucket_name}',
    #                 'EnableManifestOutput': False,
    #                 'Filter': {
    #                     'EligibleForReplication': True,
    #                 }
    #             }
    #         }
    #     )
    #     data_result.append({'src_bucket_name': src_bucket_name, 'dst_bucket_name':dst_bucket_name, 
    #                     'dst_account_id': dst_account_id, 'job_id':job_id['JobId']})

    # with open('bucket_result.csv', 'w', newline='') as file:
    #     writer = csv.writer(file)
    #     writer.writerows(data_result)
