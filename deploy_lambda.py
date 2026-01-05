#!/usr/bin/env python3
"""
Deploy Lambda function to AWS using boto3.
Uses credentials from .env file.
"""

import os
import json
import zipfile
import tempfile
import subprocess
from pathlib import Path
from dotenv import load_dotenv
import boto3

# Load environment variables
load_dotenv()

# Configuration
FUNCTION_NAME = 'es-rerank'
RUNTIME = 'python3.11'
HANDLER = 'lambda_rerank.lambda_handler'
TIMEOUT = 900  # 15 minutes
MEMORY_SIZE = 1024
REGION = os.getenv('AWS_REGION', 'me-south-1')

# AWS clients
session = boto3.Session(
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=REGION
)
lambda_client = session.client('lambda')
iam_client = session.client('iam')
events_client = session.client('events')
s3_client = session.client('s3')

S3_BUCKET = 'es-rerank-lambda-deployments'


def create_lambda_role():
    """Create IAM role for Lambda if it doesn't exist."""
    role_name = 'es-rerank-lambda-role'

    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Action": "sts:AssumeRole"
        }]
    }

    # Policy for Lambda execution + Redshift access
    policy_document = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents"
                ],
                "Resource": "arn:aws:logs:*:*:*"
            },
            {
                "Effect": "Allow",
                "Action": [
                    "redshift:GetClusterCredentials",
                    "redshift:DescribeClusters"
                ],
                "Resource": "*"
            }
        ]
    }

    try:
        # Check if role exists
        response = iam_client.get_role(RoleName=role_name)
        role_arn = response['Role']['Arn']
        print(f"Using existing role: {role_arn}")
    except iam_client.exceptions.NoSuchEntityException:
        # Create role
        print(f"Creating IAM role: {role_name}")
        response = iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description='Role for ES Rerank Lambda function'
        )
        role_arn = response['Role']['Arn']

        # Attach basic execution policy
        iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn='arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'
        )

        # Create and attach custom policy
        iam_client.put_role_policy(
            RoleName=role_name,
            PolicyName='es-rerank-policy',
            PolicyDocument=json.dumps(policy_document)
        )

        print(f"Created role: {role_arn}")
        print("Waiting for role to propagate...")
        import time
        time.sleep(10)

    return role_arn


def create_deployment_package():
    """Create Lambda deployment package with dependencies."""
    print("Creating deployment package...")

    # Create temp directory for package
    with tempfile.TemporaryDirectory() as tmpdir:
        package_dir = Path(tmpdir) / 'package'
        package_dir.mkdir()

        # Install dependencies with Linux x86_64 binaries
        print("Installing dependencies...")
        result = subprocess.run([
            'pip3', 'install',
            '-r', 'lambda_requirements.txt',
            '-t', str(package_dir),
            '--platform', 'manylinux2014_x86_64',
            '--implementation', 'cp',
            '--python-version', '3.11',
            '--only-binary=:all:',
            '--upgrade'
        ], capture_output=True, text=True)

        if result.returncode != 0:
            print("Warning: Some packages may not have pre-built wheels, trying without platform constraint...")
            subprocess.run([
                'pip3', 'install',
                '-r', 'lambda_requirements.txt',
                '-t', str(package_dir),
                '--upgrade', '--quiet'
            ], check=True)

        # Copy lambda function
        import shutil
        shutil.copy('lambda_rerank.py', package_dir)

        # Create zip
        zip_path = Path(tmpdir) / 'lambda.zip'
        print("Creating zip file...")
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            for file_path in package_dir.rglob('*'):
                if file_path.is_file():
                    arcname = file_path.relative_to(package_dir)
                    zf.write(file_path, arcname)

        # Read zip content
        with open(zip_path, 'rb') as f:
            return f.read()


def upload_to_s3(zip_content):
    """Upload deployment package to S3."""
    # Create bucket if it doesn't exist
    try:
        s3_client.head_bucket(Bucket=S3_BUCKET)
        print(f"Using existing S3 bucket: {S3_BUCKET}")
    except:
        print(f"Creating S3 bucket: {S3_BUCKET}")
        if REGION == 'us-east-1':
            s3_client.create_bucket(Bucket=S3_BUCKET)
        else:
            s3_client.create_bucket(
                Bucket=S3_BUCKET,
                CreateBucketConfiguration={'LocationConstraint': REGION}
            )

    # Upload zip
    s3_key = f'{FUNCTION_NAME}/lambda.zip'
    print(f"Uploading to s3://{S3_BUCKET}/{s3_key}...")
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=zip_content
    )
    return s3_key


def deploy_lambda(role_arn, zip_content):
    """Deploy or update Lambda function."""

    # Upload to S3 first (package > 50MB)
    s3_key = upload_to_s3(zip_content)

    env_vars = {
        'ELASTICSEARCH_HOST': os.getenv('ELASTICSEARCH_HOST'),
        'ELASTICSEARCH_PORT': os.getenv('ELASTICSEARCH_PORT', '9243'),
        'ELASTICSEARCH_USERNAME': os.getenv('ELASTICSEARCH_USERNAME'),
        'ELASTICSEARCH_PASSWORD': os.getenv('ELASTICSEARCH_PASSWORD'),
        'REDSHIFT_HOST': os.getenv('REDSHIFT_HOST'),
        'REDSHIFT_PORT': os.getenv('REDSHIFT_PORT', '5439'),
        'REDSHIFT_DATABASE': os.getenv('REDSHIFT_DATABASE'),
        'REDSHIFT_USER': os.getenv('REDSHIFT_USER'),
        'REDSHIFT_CLUSTER_ID': os.getenv('REDSHIFT_CLUSTER_ID', 'jazi-datawarehouse-cluster'),
        'ES_INDEX': os.getenv('ES_INDEX', 'skus_product_pool_v3'),
        'MAX_SCORE': '100',
        'FACTOR': '30'
    }

    try:
        # Try to update existing function
        print(f"Updating Lambda function: {FUNCTION_NAME}")
        lambda_client.update_function_code(
            FunctionName=FUNCTION_NAME,
            S3Bucket=S3_BUCKET,
            S3Key=s3_key
        )

        # Wait for update to complete
        waiter = lambda_client.get_waiter('function_updated')
        waiter.wait(FunctionName=FUNCTION_NAME)

        # Update configuration
        lambda_client.update_function_configuration(
            FunctionName=FUNCTION_NAME,
            Runtime=RUNTIME,
            Handler=HANDLER,
            Timeout=TIMEOUT,
            MemorySize=MEMORY_SIZE,
            Environment={'Variables': env_vars}
        )

        response = lambda_client.get_function(FunctionName=FUNCTION_NAME)
        function_arn = response['Configuration']['FunctionArn']
        print(f"Updated function: {function_arn}")

    except lambda_client.exceptions.ResourceNotFoundException:
        # Create new function
        print(f"Creating Lambda function: {FUNCTION_NAME}")
        response = lambda_client.create_function(
            FunctionName=FUNCTION_NAME,
            Runtime=RUNTIME,
            Role=role_arn,
            Handler=HANDLER,
            Code={'S3Bucket': S3_BUCKET, 'S3Key': s3_key},
            Timeout=TIMEOUT,
            MemorySize=MEMORY_SIZE,
            Environment={'Variables': env_vars},
            Description='Rerank products based on view counts from Redshift'
        )
        function_arn = response['FunctionArn']
        print(f"Created function: {function_arn}")

    return function_arn


def create_schedule(function_arn):
    """Create EventBridge rule to trigger Lambda daily."""
    rule_name = 'es-rerank-daily'

    print(f"Creating EventBridge schedule: {rule_name}")

    # Create rule (daily at 2 AM UTC)
    events_client.put_rule(
        Name=rule_name,
        ScheduleExpression='cron(0 2 * * ? *)',
        State='ENABLED',
        Description='Trigger ES rerank Lambda daily at 2 AM UTC'
    )

    # Get account ID from function ARN
    account_id = function_arn.split(':')[4]

    # Add Lambda permission for EventBridge
    try:
        lambda_client.add_permission(
            FunctionName=FUNCTION_NAME,
            StatementId='EventBridgeInvoke',
            Action='lambda:InvokeFunction',
            Principal='events.amazonaws.com',
            SourceArn=f'arn:aws:events:{REGION}:{account_id}:rule/{rule_name}'
        )
    except lambda_client.exceptions.ResourceConflictException:
        pass  # Permission already exists

    # Add target
    events_client.put_targets(
        Rule=rule_name,
        Targets=[{
            'Id': 'es-rerank-lambda',
            'Arn': function_arn,
            'Input': json.dumps({'max_score': 100, 'factor': 30})
        }]
    )

    print(f"Schedule created: Daily at 2 AM UTC")


def main():
    print("=" * 50)
    print("Deploying ES Rerank Lambda")
    print("=" * 50)
    print(f"Region: {REGION}")
    print(f"Function: {FUNCTION_NAME}")
    print()

    # Step 1: Create/get IAM role
    role_arn = create_lambda_role()

    # Step 2: Create deployment package
    zip_content = create_deployment_package()
    print(f"Package size: {len(zip_content) / 1024 / 1024:.2f} MB")

    # Step 3: Deploy Lambda
    function_arn = deploy_lambda(role_arn, zip_content)

    # Step 4: Create schedule
    create_schedule(function_arn)

    print()
    print("=" * 50)
    print("Deployment complete!")
    print("=" * 50)
    print(f"Function ARN: {function_arn}")
    print(f"Schedule: Daily at 2 AM UTC")
    print()
    print("Test manually:")
    print(f"  aws lambda invoke --function-name {FUNCTION_NAME} --payload '{{\"dry_run\": true}}' output.json --region {REGION}")
    print()


if __name__ == '__main__':
    main()
