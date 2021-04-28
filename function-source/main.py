bucket_name = 'sba_data_uoftcloudrj'
model_name = 'model.pkl'
prediction_file = 'predict_via_cloudfunction.csv'

from google.cloud import storage
import pandas as pd
import pickle
import os

def load_model(bucket_name, model_name, prediction_file):
  storage_client = storage.Client()
  public_bucket = storage_client.bucket(bucket_name)
  blob = public_bucket.blob(model_name)
  folder = '/tmp/'
  if not os.path.exists(folder):
    os.makedirs(folder)
  blob.download_to_filename(folder + model_name)
  blob = public_bucket.blob(prediction_file)
  blob.download_to_filename(folder + prediction_file)

def get_prediction_data():
  prediction_data = pd.read_csv('/tmp/' + prediction_file, index_col='LoanNr_ChkDgt')
  global df
  df = pd.DataFrame(prediction_data)
  return df

def predict():
  with open('/tmp/model.pkl','rb') as f:
    model = pickle.load(f)
  #model = pickle.load(open('/tmp/' + model_name,'rb'))
  result = model.predict(df)
  df['prediction'] = result
  df['prediction'] = df['prediction'].map({0:'paid in full',1:"charged off"})
  global pred_var
  pred_var = df.to_dict()
  return df['prediction']

def hello_world(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """
    request_json = request.get_json()
    if request.args and 'message' in request.args:
        return request.args.get('message')
    elif request_json and 'message' in request_json:
        return request_json['message']
    else:
        print("1")
        load_model(bucket_name, model_name, prediction_file)
        print("2")
        get_prediction_data()
        predict()
        print(df['prediction'])
        print(pred_var)
        return pred_var
        return f'Hello World!'


#def load_prediction_data(bucket_name, blob_text, destination_blob_name):
#  storage_client = storage.Client()
#  public_bucket = storage_client.get_bucket(bucket_name)
#  blob = public_bucket.blob(destination_blob_name)
#  blob.upload_from_string(blob_text)
#  print('File {} uploaded to bucket {}'.format(source_file_name,destination_blob_name))


