import os

def hello_gcs():
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    from google.cloud import automl, storage
    import base64
    import os

    # TODO(developer): Uncomment and set the following variables
    project_id = "hackathon-team-01"
    model_id = "ICN7438396273020895232"

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


if __name__=='__main__':
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="C:\\Users\\Msand\\Downloads\\hackathon-team-01-5bb753db14f4.json"
    hello_gcs()