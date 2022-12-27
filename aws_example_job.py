from dagster import job

from ops.pexels import download_random_video_from_pexels
from ops.aws.s3 import upload_video_to_s3
from ops.lightly_run import schedule_lightly_run
from ops.aws.ec2 import run_lightly_onprem


@job
def aws_example_job():
    """Example data processing pipeline with Lightly on AWS.

    The pipeline performs the following three steps:
        - Download a random video from pexels
        - Upload the video to an s3 bucket
        - Run docker worker
        - Run Lightly selection on the video using CORESET
        - Shutdown the instance

    """
    file_name = download_random_video_from_pexels()
    object_name = upload_video_to_s3(file_name)
    run_lightly_onprem(object_name)
    message = schedule_lightly_run()
    shutdown_instance(message)