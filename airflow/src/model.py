import gridfs
import os
import sys
from gridfs import GridFS
import joblib
from bson import ObjectId
from pymongo import MongoClient
import tensorflow.keras.backend as K
import io
import pickle
import tensorflow as tf
import time
import scipy.io

this_dir=os.path.dirname(__file__)
tf.saved_model.LoadOptions(experimental_io_device=this_dir)
class ModelSingleton(type):
    """
    Metaclass that creates a Singleton base type when called.
    """
    _mongo_id = {}
    def __call__(cls, *args, **kwargs):
        mongo_id = kwargs['mongo_id']
        if mongo_id not in cls._mongo_id:
            print('Adding model into ModelSingleton')
            cls._mongo_id[mongo_id] = super(ModelSingleton, cls).__call__(*args, **kwargs)
        return cls._mongo_id[mongo_id]
   
class LoadModel(metaclass=ModelSingleton):
    def __init__(self, *args, **kwargs):
        print(kwargs)
        self.mongo_id = kwargs['mongo_id']
        self.clf = self.load_model()
    def load_model(self):
        print('loading model')
        #host = Variable.get("MONGO_URL_SECRET")
        #    host = os.environ['MONGO_URL_SECRET'] 
        # host = "mongodb://root:coops2022@mongodb-0.mongodb-headless.mongo.svc.cluster.local:27017, mongodb-1.mongodb-headless.mongo.svc.cluster.local:27017"
        host = "mongodb://114.200.143.112:27017"
        client = MongoClient(host)

        db_model = client['model']
        fs = gridfs.GridFS(db_model)
        print(self.mongo_id)
        f = fs.find({"_id": ObjectId(self.mongo_id)}).next()
        print(f)
        print(f.model_name)
        print(f.uploadDate)
        fullpath = os.path.join(this_dir,f"{f.model_name}.h5")
        print(fullpath)
        with open(fullpath, 'wb') as outfile:
            outfile.write(f.read())
        return tf.keras.models.load_model(fullpath)
        # return joblib.load(fullpath)

def LoadfromMongo(model_name,collection_name):
    json_data={}
    # host = "mongodb://root:coops2022@mongodb-0.mongodb-headless.mongo.svc.cluster.local:27017, mongodb-1.mongodb-headless.mongo.svc.cluster.local:27017"
    host = "mongodb://114.200.143.112:27017"
    client=MongoClient(host)
    db_model = client['model']
    collection_model = db_model[collection_name]
    data = collection_model.find({'name':model_name})
    for i in data:
        json_data=i
    pickled_model=json_data[model_name]
    print(pickled_model)
    print(type(pickled_model))
    return pickle.loads(pickled_model,encoding='bytes')


def SaveModel(model,collection_name,model_name,train_dt,loss=None,local=False):
    print('saving model...')
    # host = os.environ['MONGO_URL_SECRET']
    # host = "mongodb://root:coops2022@mongodb-0.mongodb-headless.mongo.svc.cluster.local:27017, mongodb-1.mongodb-headless.mongo.svc.cluster.local:27017"
    host = "mongodb://114.200.143.112:27017"
    # print(host)
    client=MongoClient(host)
    db_model = client['model']
    fs = gridfs.GridFS(db_model)
    collection_model = db_model[collection_name]
    model_fpath = f'{model_name}.h5'
    # joblib.dump(model, model_fpath)
    model.save(model_fpath)
    # save the local file to mongodb
    with open(model_fpath, 'rb') as infile:
        file_id = fs.put(
            infile.read(),
            model_name=model_name
        )
        # insert the model status info to ModelStatus collection
            
        params = {
            'model_name': model_name,
            'file_id': file_id,
            'inserted_time': train_dt
        }
        # if loss is not None:
        #     params = {
        #     'model_name': model_name,
        #     'file_id': file_id,
        #     'inserted_time': train_dt,
        #     'loss' : loss
        #     }
        result = collection_model.insert_one(params)

    # pickled_model =pickle.dumps(model)

    # info = collection_model.insert_one({model_name:pickled_model,'name':model_name,'created_time':time.time()})
    # print(info.inserted_id,' id')

    client.close()
    return result