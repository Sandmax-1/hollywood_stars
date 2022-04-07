import os
from google.cloud import automl, storage, bigquery
import base64


def hello_gcs(project_id, model_id):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """

    # TODO(developer): Uncomment and set the following variables

    prediction_client = automl.PredictionServiceClient()

    # Get the full path of the model.
    model_full_id = automl.AutoMlClient.model_path(project_id, "us-central1", model_id)

    # # Read the file.
    # storage_client = storage.Client()
    # bucket = storage_client.bucket(event["bucket"])
    # blob = bucket.blob(event["name"])
    # blob.download_to_filename("/tmp/image.jpg")

    # file_path = "/tmp/image.jpg"

    file_path = "C:\\Users\\Msand\\repos\\hollywood_stars\\ben_affleck.jpg"

    with open(file_path, "rb") as f:
        content = f.read()
    # print(content)
    # print(base64.b64encode(content))

    #   payload = {"image": {
    #     "imageBytes": base64.b64encode(content)
    #   }
    # }

    # image = automl.Image(image_bytes=base64.b64encode(content))
    print("setting the image..")
    image = automl.Image(image_bytes=content)
    print("setting the payload..")
    payload = automl.ExamplePayload(image=image)
    # print(automl.ExamplePayload(image=image))

    # params is additional domain-specific parameters.
    # score_threshold is used to filter the result
    # https://cloud.google.com/automl/docs/reference/rpc/google.cloud.automl.v1#predictrequest
    params = {"score_threshold": "0.8"}
    print("getting the request...")

    request = automl.PredictRequest(name=model_full_id, payload=payload, params=params)
    print("got the request")
    print("getting the response")
    response = prediction_client.predict(request=request)

    print("Prediction results:")
    for result in response.payload:
        print("Predicted class name: {}".format(result.display_name))
        print("Predicted class score: {}".format(result.classification.score))
    return result


def make_bigquery_table(project_id, dataset_name, table_name):
    make_bigquery_dataset(dataset_name)
    make_bigquery_table(project_id, table_name)


def make_bigquery_dataset(dataset_name):
    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set dataset_id to the ID of the dataset to create.
    dataset_id = f"{client.project}.{dataset_name}"

    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)

    # TODO(developer): Specify the geographic location where the dataset should reside.
    dataset.location = "US"

    # Send the dataset to the API for creation, with an explicit timeout.
    # Raises google.api_core.exceptions.Conflict if the Dataset already
    # exists within the project.
    dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
    print(f"Created dataset {client.project}.{dataset.dataset_id}")


def make_bigquery_table(project_id, table_name):
    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    table_id = f"{project_id}.maxs_test_dataset.maxs_legendary_table"

    schema = [
        bigquery.SchemaField("result", "STRING", mode="REQUIRED"),
    ]

    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)  # Make an API request.
    print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")


def insert_into_table(project_id, dataset_name, table_name, result):
    from google.cloud import bigquery

    bq_client = bigquery.Client()
    table = bq_client.get_table(f"{project_id}.{dataset_name}.{table_name}")

    rows_to_insert = [{"result": result.classification.score}]

    errors = bq_client.insert_rows_json(table, rows_to_insert)
    if errors == []:
        print("success")


if __name__ == "__main__":
    os.environ[
        "GOOGLE_APPLICATION_CREDENTIALS"
    ] = "C:\\Users\\Msand\\Downloads\\hackathon-team-01-5bb753db14f4.json"
    project_id = "hackathon-team-01"
    model_id = "ICN7438396273020895232"
    result = hello_gcs(project_id, model_id)
    first_run = False
    dataset_name = "maxs_test_dataset"
    table_name = "maxs_legendary_table"
    if first_run:
        make_bigquery_table(project_id, dataset_name, table_name, result)

    insert_into_table(project_id, dataset_name, table_name, result)
