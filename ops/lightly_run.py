import json
from dagster import op
from lightly.api import ApiWorkflowClient
from lightly.openapi_generated.swagger_client.models.dataset_type import DatasetType
from lightly.openapi_generated.swagger_client.models.datasource_purpose import DatasourcePurpose


with open("config/config.json", "r") as jsonfile:
    configs_json = json.load(jsonfile)
    
LIGHTLY_TOKEN = configs_json["LIGHTLY_TOKEN"]
DATASET_NAME = configs_json["DATASET_NAME"]
S3_REGION = configs_json["S3_REGION"]
S3_ROLE_ARN = configs_json["S3_ROLE_ARN"]
S3_EXTERNAL_ID = configs_json["S3_EXTERNAL_ID"]
S3_INPUT_BUCKET = configs_json["S3_INPUT_BUCKET"]
S3_LIGHTLY_BUCKET = configs_json["S3_LIGHTLY_BUCKET"]

class LightlyRun:
    """Pexels client to download a random popular video. """

    def __init__(
        self,
        lightly_token = LIGHTLY_TOKEN,
        dataset_name = DATASET_NAME,
        s3_region = S3_REGION,
        s3_role_arn = S3_ROLE_ARN,
        s3_external_id = S3_EXTERNAL_ID,
        s3_input_bucket = S3_INPUT_BUCKET,
        s3_lightly_bucket = S3_LIGHTLY_BUCKET

    ):
        self.lightly_token = lightly_token
        self.dataset_name = dataset_name
        self.s3_region = s3_region
        self.s3_role_arn = s3_role_arn
        self.s3_external_id = s3_external_id
        self.s3_input_bucket = s3_input_bucket
        self.s3_lightly_bucket = s3_lightly_bucket
        self.client = ApiWorkflowClient(token=lightly_token)

    def create_dataset(self) -> None:
        try:
            self.client.create_dataset(
                self.dataset_name,
                DatasetType.VIDEOS,
            )
            print(f"Created dataset with name: {self.dataset_name}")
        except ValueError:
            self.client.set_dataset_id_by_name(self.dataset_name)
            print(f"Dataset with name {self.dataset_name} already exists. Adding images to datapool.")
        
        return
    
    def set_s3_config(self) -> None:

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

    def schedule_run(self) -> str:
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
    
    def monitor_run(self, scheduleId: str) -> None:
        for run_info in self.client.compute_worker_run_info_generator(scheduled_run_id=scheduleId):
            print(f"Compute worker run is now in state='{run_info.state}' with message='{run_info.message}'")


@op
def schedule_lightly_run() -> str:
    """Dagster op to schedule a Lightly run.

    """

    lightlyInstance = LightlyRun()
    lightlyInstance.create_dataset()
    lightlyInstance.set_s3_config()
    scheduleId = lightlyInstance.schedule_run()
    lightlyInstance.monitor_run(scheduleId)
    
    return "Done"