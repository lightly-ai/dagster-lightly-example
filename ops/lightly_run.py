from dagster import op
from lightly.api import ApiWorkflowClient
from lightly.openapi_generated.swagger_client.models.dataset_type import DatasetType
from lightly.openapi_generated.swagger_client.models.datasource_purpose import DatasourcePurpose

LIGHTLYTOKEN = "MY_LIGHTLY_TOKEN"
DATASETNAME = "dataset-name"
S3REGION = "eu-central-1"
S3ROLEARN = "S3-ACCESS-KEY"
S3SEXTERNALID = "S3-SECRET-ACCESS-KEY"
S3INPUTBUCKET= "s3://bucket/input/"
S3LIGHTLYBUCKET= "s3://bucket/lightly/"

class LightlyRun:
    """Pexels client to download a random popular video. """

    def __init__(
        self,
        lightly_token = LIGHTLYTOKEN,
        dataset_name = DATASETNAME,
        s3_region = S3REGION,
        s3_role_arn = S3ROLEARN,
        s3_external_id = S3SEXTERNALID,
        s3_input_bucket = S3INPUTBUCKET,
        s3_lightly_bucket = S3LIGHTLYBUCKET

    ):
        self.lightly_token = lightly_token
        self.dataset_name = dataset_name
        self.s3_region = s3_region
        self.s3_role_arn = s3_role_arn
        self.s3_external_id = s3_external_id
        self.s3_input_bucket = s3_input_bucket
        self.s3_lightly_bucket = s3_lightly_bucket
        self.client = ApiWorkflowClient(token=lightly_token)

    def createDataset(self) -> None:
        self.client.create_dataset(
            self.dataset_name,
            DatasetType.VIDEOS  
        )
        return
    
    def setS3Config(self) -> None:

        # Input bucket
        self.client.set_s3_delegated_access_config(
            resource_path=self.s3_input_bucket,
            region=self.s3_region,
            role_arn=self.s3_role_arn,
            external_id=self.s3_external_id,
            purpose=DatasourcePurpose.INPUT
        )

        # Lightly bucket
        self.client.set_s3_delegated_access_config(
            resource_path=self.s3_lightly_bucket,
            region=self.s3_region,
            role_arn=self.s3_role_arn,
            external_id=self.s3_external_id,
            purpose=DatasourcePurpose.LIGHTLY
        )

        return

    def scheduleRun(self) -> str:
        scheduleId = self.client.schedule_compute_worker_run(
            selection_config={
                "proportion_samples": 0.1,
                "strategies": [
                    {
                        "input": {
                            "type": "EMBEDDINGS",
                        },
                        "strategy": {
                            "type": "DIVERSITY",
                        }
                    }
                ]
            },
        )

        return scheduleId
    
    def monitorRun(self, scheduleId: str) -> None:
        for run_info in self.client.compute_worker_run_info_generator(scheduled_run_id=scheduleId):
            print(f"Compute worker run is now in state='{run_info.state}' with message='{run_info.message}'")


@op
def schedule_lightly_run() -> str:
    """Dagster op to schedule a Lightly run.

    """

    lightlyInstance = LightlyRun()
    lightlyInstance.createDataset()
    lightlyInstance.setS3Config()
    scheduleId = lightlyInstance.scheduleRun()
    lightlyInstance.monitorRun(scheduleId)
    
    return "Done"