import sys
import os

from google.cloud import automl_v1beta1
from google.cloud.automl_v1beta1.proto import service_pb2


# 'content' is base-64-encoded image data.
def get_prediction(content, project_id, model_id):
    prediction_client = automl_v1beta1.PredictionServiceClient()    
    name = 'projects/{}/locations/us-central1/models/{}'.format(project_id, model_id)
    payload = {'image': {'image_bytes': content }}
    params = {}
    request = prediction_client.predict(name, payload, params)
    return request  # waits till request is returned

if __name__ == '__main__':
    file_path = os.environ['FILE_PATH'] #sys.argv[1]
    project_id = os.environ['PROJECT_ID'] #sys.argv[2]
    model_id = os.environ['MODEL_ID'] #sys.argv[3]  
    with open(file_path, 'rb') as ff:
      content = ff.read()   
    print get_prediction(content, project_id, model_id)