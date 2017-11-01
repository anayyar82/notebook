# Databricks notebook source
import tensorflow as tf

from keras import backend as K

from keras.applications.resnet50 import ResNet50
from keras.preprocessing import image
from keras.applications.resnet50 import preprocess_input, decode_predictions
import numpy as np


def process_partition(partition_id, partition):
    with GPULock() as gpu_id:
        print('Initializing Session')
        # This is where it is supposed to get stuck. in the initialization of the TF session
        config = tf.ConfigProto()
        config.gpu_options.per_process_gpu_memory_fraction = 0.9
        config.gpu_options.visible_device_list = str(gpu_id)
        K.set_session(tf.Session(config=config))
        
        print('Simulate some load on the GPUs') 
        # Standard "Hello World" code from https://keras.io/applications/
        model = ResNet50(weights='imagenet')
        img = image.load_img('/dbfs/ImageLab/test-image.jpg', target_size=(224, 224))
        x = image.img_to_array(img)
        x = np.expand_dims(x, axis=0)
        x = preprocess_input(x)
        preds = model.predict(x)
       
        print('Closing Session')
        K.get_session().close()
        
        return [decode_predictions(preds, top=3)[0]]

# COMMAND ----------

from pyspark import TaskContext
import requests
import json

app_id = sc.applicationId
base_url = sc.uiWebUrl

def get_executor_id():
  tc = TaskContext.get()
  url = base_url+'/api/v1/applications/'+app_id+'/stages/'+str(tc.stageId())
  stages = json.loads(requests.get(url).content)[0]
  task = stages['tasks'][str(tc.taskAttemptId())]
  #yield int(task['executorId'])
  yield (task['host'], int(task['executorId']), task['taskId'])

sc.parallelize(range(1024), 16).mapPartitions(lambda iter: get_executor_id()).collect()

# COMMAND ----------

