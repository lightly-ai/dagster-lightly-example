import os
import string
import random

import boto3
from botocore.exceptions import ClientError

from dagster import op


S3INPUTBUCKET: str = "inputbucket"
REGION_NAME: str = 'eu-central-1'


class S3Client:
    """S3 client to upload files to a bucket.

    """

    def __init__(
        self,
        s3_input_bucket = S3INPUTBUCKET,
        region_name = REGION_NAME
    ):
        self.s3_input_bucket = s3_input_bucket
        self.region_name = region_name
        self.s3 = boto3.client('s3', region_name=self.region_name)


    def random_subfolder(self, size_: int = 8):
        """Generates a random subfolder name of uppercase letters and digits.

        """
        chars = string.ascii_uppercase + string.digits
        return ''.join(random.choice(chars) for _ in range(size_))


    def upload_file(self, filename: str):
        """Uploads the file at filename to the s3 bucket.

        Generates a random subfolder so the file will be stored at:
        >>> BUCKET_NAME/RANDOM_SUBFOLDER/basefilename.mp4

        """

        # upload file to lightly-aws-bucket/input_dir/RANDOM_STRING/basename.mp4
        object_name = os.path.join(
            'input_dir',
            self.random_subfolder(),
            os.path.basename(filename)
        )

        # Upload the file
        try:
            self.s3.upload_file(filename, self.s3_input_bucket, ("dagster_example/" + object_name))
        except ClientError as e:
            print(e)
            return None

        return object_name


@op
def upload_video_to_s3(filename: str) -> str:
    """Dagster op to upload a video to an s3 bucket.

    Args:
        filename:
            Path to the video which should be uploaded.

    Returns:
        The name of the object in the s3 bucket.

    """

    s3_client = S3Client()
    object_name = s3_client.upload_file(filename)

    return object_name