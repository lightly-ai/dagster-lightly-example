import os
import time
import json

import boto3
from botocore.exceptions import ClientError

from dagster import op

with open("config/config.json", "r") as jsonfile:
    configs_json = json.load(jsonfile)

REGION_NAME = configs_json['S3REGION']
INSTANCE_ID = configs_json['INSTANCE_ID']
SCRIPTPATH = configs_json['SCRIPTPATH']

class EC2Client:
    """EC2 client to start, run, and stop instances.

    """

    def __init__(self,
        region_name = REGION_NAME,
        instance_id = INSTANCE_ID,
  
        ):
        self.region_name = region_name
        self.instance_id = instance_id
 
        self.ec2 = boto3.client('ec2', region_name=self.region_name)
        self.ssm = boto3.client('ssm', region_name=self.region_name)


    def wait(self, client, wait_for: str, **kwargs):
        """Waits for a certain status of the ec2 or ssm client.

        """
        waiter = client.get_waiter(wait_for)
        waiter.wait(**kwargs)
        print(f'{wait_for}: OK')


    def start_instance(self):
        """Starts the EC2 instance with the given id.

        """
        # Do a dryrun first to verify permissions
        try:
            self.ec2.start_instances(
                InstanceIds=[self.instance_id],
                DryRun=True
            )
        except ClientError as e:
            if 'DryRunOperation' not in str(e):
                raise

        # Dry run succeeded, run start_instances without dryrun
        try:
            self.ec2.start_instances(
                InstanceIds=[self.instance_id],
                DryRun=False
            )
        except ClientError as e:
            print(e)

        self.wait(self.ec2, 'instance_exists')
        self.wait(self.ec2, 'instance_running')

    
    def stop_instance(self):
        """Stops the EC2 instance with the given id.

        """
        # Do a dryrun first to verify permissions
        try:
            self.ec2.stop_instances(
                InstanceIds=[self.instance_id],
                DryRun=True
            )
        except ClientError as e:
            if 'DryRunOperation' not in str(e):
                raise

    def run_command(self, command: str):
        """Runs the given command on the instance with the given id.

        """

        # Make sure the instance is OK
        time.sleep(20)
        
        response = self.ssm.send_command(
            DocumentName='AWS-RunShellScript',
            Parameters={'commands': [command]},
            InstanceIds=[self.instance_id]
        )
        command_id = response['Command']['CommandId']

        # Make sure the command is pending
        time.sleep(10)
        
        import pprint
        pprint.pprint(
            self.ssm.get_command_invocation(
                CommandId=command_id,
                InstanceId=self.instance_id,
            )
        )


@op
def run_lightly_onprem(object_name: str) -> str:
    """Dagster op to run Lightly On-premise on a remote EC2 instance.

    Args:
        object_name:
            S3 object containing the input video(s) for Lightly.

    """

    # object name is of format path/RANDOM_DIR/RANDOM_NAME.mp4
    # so the input directory is the RANDOM_DIR

    ec2_client = EC2Client()
    ec2_client.start_instance()
    ec2_client.run_command('bash ' + SCRIPTPATH)
    
@op
def shutdown_instance(message: str) -> None:
    """Dagster op to run Lightly On-premise on a remote EC2 instance.

    Args:
        message:
            Finished Lightly run message

    """

    if message == "Done":
        ec2_client = EC2Client()
        ec2_client.stop_instance()