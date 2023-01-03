import json

config = {
    "PEXELS_API_KEY" : 'YOUR_PEXELS_API_KEY',
    "LIGHTLYTOKEN" : "MY_LIGHTLY_TOKEN",
    "DATASETNAME" : "dataset-name",
    "S3REGION" : "eu-central-1",
    "S3ROLEARN" : "S3-ACCESS-KEY",
    "S3SEXTERNALID" : "S3-SECRET-ACCESS-KEY",
    "S3INPUTBUCKET" : "s3://bucket/input/",
    "S3LIGHTLYBUCKET" : "s3://bucket/lightly/",
    "INSTANCE_ID" : "YOUR-INSTANCE-ID",

}
myJSON = json.dumps(config, indent=4)

with open("config.json", "w") as jsonfile:
    jsonfile.write(myJSON)
    print("Write successful")