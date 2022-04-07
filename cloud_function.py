import os
from google.cloud import automl, storage, bigquery
import base64
from datetime import datetime


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

    file_path = "C:\\Users\\Msand\\repos\\hollywood_stars\\rock.jpg"

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
        print(f"Predicted class name: {result.display_name}")
        print(f"Predicted class score: {result.classification.score}")
    return result


def create_dataset_and_table(project_id, dataset_name, table_name):
    try:
        make_bigquery_dataset(dataset_name)
    except Exception as e:
        msg = str(e)
        if "Already Exists" not in msg:
            raise
    try:
        make_bigquery_table(project_id, dataset_name, table_name)
    except Exception as e:
        msg = str(e)
        if "Already Exists" not in msg:
            raise


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


def make_bigquery_table(project_id, dataset_name, table_name):
    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    table_id = f"{project_id}.{dataset_name}.{table_name}"

    schema = [
        bigquery.SchemaField("actual_person", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("predicted_person", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("score", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("correct", "BOOL", mode="REQUIRED"),
        bigquery.SchemaField("time", "TIMESTAMP", mode="REQUIRED"),
    ]

    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)  # Make an API request.
    print(f"Created table {table_id}")


def insert_into_table(project_id, dataset_name, table_name, result):
    from google.cloud import bigquery

    bq_client = bigquery.Client()
    table = bq_client.get_table(f"{project_id}.{dataset_name}.{table_name}")

    actual_person = "ben_affleck"
    predicted_person = result.display_name

    rows_to_insert = [
        {
            "actual_person": actual_person,
            "predicted_person": predicted_person,
            "score": result.classification.score,
            "correct": actual_person == predicted_person,
            "time": str(datetime.now()),
        }
    ]

    errors = bq_client.insert_rows_json(table, rows_to_insert)
    if errors == []:
        print("success")


def run_the_lot(project_id, model_id, dataset_name, table_name):
    result = hello_gcs(project_id, model_id)

    create_dataset_and_table(project_id, dataset_name, table_name)

    insert_into_table(project_id, dataset_name, table_name, result)


if __name__ == "__main__":
    os.environ[
        "GOOGLE_APPLICATION_CREDENTIALS"
    ] = "C:\\Users\\Msand\\Downloads\\hackathon-team-01-5bb753db14f4.json"
    project_id = "hackathon-team-01"
    model_id = "ICN7438396273020895232"
    dataset_name = "model_results"
    table_name = "model_results_table"

    run_the_lot(
        project_id=project_id,
        model_id=model_id,
        dataset_name=dataset_name,
        table_name=table_name,
    )
