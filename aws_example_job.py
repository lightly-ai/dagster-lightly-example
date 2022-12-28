from dagster import job

from ops.pexels import download_random_video_from_pexels
from ops.aws.s3 import upload_video_to_s3
from ops.lightly_run import schedule_lightly_run
from ops.aws.ec2 import run_lightly_onprem
from ops.aws.ec2 import shutdown_instance


@job
def aws_example_job():
    """Example data processing pipeline with Lightly on AWS.

    The pipeline performs the following three steps:
        - Download a random video from Pexels.
        - Upload the video to an S3 bucket.
        - Spin up an EC2 instance and start the Lightly Worker waiting for a job to process.
        - Schedule a run for the Lightly Worker and wait till it has finished.
        - Shutdown the instance
    """
    file_name = download_random_video_from_pexels()
    object_name = upload_video_to_s3(file_name)
    run_lightly_onprem(object_name)
    message = schedule_lightly_run()
    shutdown_instance(message)