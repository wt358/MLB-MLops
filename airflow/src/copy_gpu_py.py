from datetime import timedelta
from datetime import datetime
import statsapi
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.decomposition import PCA
from sklearn.metrics import precision_score, recall_score, f1_score,accuracy_score, classification_report,  confusion_matrix

from pymongo import MongoClient, ASCENDING, DESCENDING, TEXT
from sklearn.model_selection import KFold, GridSearchCV
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import IsolationForest
from sklearn.svm import OneClassSVM 
from sklearn.metrics import confusion_matrix, roc_curve, roc_auc_score
from numba import cuda
from functools import partial
from scipy import integrate, stats
import matplotlib.pyplot as plt
import seaborn as sns
from bson import ObjectId

import gridfs
import io

from gridfs import GridFS



from imblearn.over_sampling import SMOTE
from imblearn.pipeline import Pipeline

from tensorflow.python.client import device_lib
from sklearn.utils import shuffle
import tensorflow as tf
from tensorflow.keras import regularizers
from tensorflow.keras.layers import Input, Dense, LSTM, TimeDistributed, RepeatVector
from tensorflow.keras.layers import BatchNormalization, Activation, Embedding, ZeroPadding2D, LeakyReLU
from tensorflow.keras.initializers import RandomNormal
from tensorflow.keras.models import  Model,Sequential

import joblib

tf.compat.v1.logging.set_verbosity(tf.compat.v1.logging.ERROR)

import csv
import pandas as pd
import os
import sys
import math

import time
import numpy as np

from collections import Counter


from kafka import KafkaConsumer
from kafka import KafkaProducer

from pymongo import MongoClient

import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
import tensorflow.keras as keras

import json
from json import loads
import random as rn

from model import *


learnig_rate = 0.0001
N_EPOCHS = 50
N_BATCH = 32
N_DROPOUT = 0.2
dict_code={'AZ':109, 'WSH':120, 'LAA' : 108 }
host = "mongodb://root:coops2022@mongodb-0.mongodb-headless.mongo.svc.cluster.local:27017, mongodb-1.mongodb-headless.mongo.svc.cluster.local:27017"

def create_model(input):
    model = keras.Sequential()
    model.add(keras.layers.Flatten(input_shape=(input,)))
    model.add(keras.layers.Dense(units=512,activation='relu'))
    model.add(keras.layers.Dropout(N_DROPOUT))
    model.add(keras.layers.Dense(units=256,activation='relu'))
    model.add(keras.layers.Dropout(N_DROPOUT))
    model.add(keras.layers.Dense(units=128,activation='relu'))
    model.add(keras.layers.Dropout(N_DROPOUT))
    model.add(keras.layers.Dense(units=64,activation='relu'))
    model.add(keras.layers.Dropout(N_DROPOUT))
    model.add(keras.layers.Dense(units=32,activation='relu'))
    model.add(keras.layers.Dropout(N_DROPOUT))
    model.add(keras.layers.Dense(units=1))

    return model

def IQR(section):
    section=section.apply(pd.to_numeric,errors='coerce')
    percent=0.05
    for i in range(len(section.columns)):
        level_1q = section.iloc[:,i].quantile(percent)
        level_3q = section.iloc[:,i].quantile(1-percent)
        IQR = level_3q - level_1q
        rev_range = 1.5 # 제거 범위 조절 변수
        section = section[(section.iloc[:,i] <= level_3q + (rev_range * IQR)) & (section.iloc[:,i] >= level_1q - (rev_range * IQR))] ## sectiond에 저장된 데이터 프레임의 이상치 제거 작업
    return section

def score_model():
    client=MongoClient(host)
    db_att=client['mov_avg_attack_y']

    col_list=db_att.list_collection_names()
    list_att=[]

    learn_date=datetime.datetime(2023,1,1)

    query={
        "game_date":{
            '$lt':learn_date
        }
    }
    for coll in col_list:
        try:
            data_att=list(db_att[coll].find(query))
        except Exception as e:
            print(e)
        list_att.extend(data_att)
    client.close()
    data_all_att=pd.DataFrame(list_att).drop(columns='_id').dropna()


    print(data_all_att.describe())
    data_cor_att=data_all_att.drop(columns=['game_date','spID','game_id'])

    # data_minmax=(data_cor-data_cor.min())/(data_cor.max()-data_cor.min())
    # print(data_minmax)
    # data_cor['OPS']
    data_cor_att=IQR(data_cor_att)

    print(data_cor_att)
    print(data_cor_att.describe())
    print(data_cor_att.corrwith(data_cor_att['runs_y']))

    data_tt_att=data_cor_att.drop(columns=['runs_y'])

    # minmax scaling
    data_tt_att=(data_tt_att-data_tt_att.min())/(data_tt_att.max()-data_tt_att.min())
    print(data_tt_att)


    # Attack model learning
    print(data_tt_att.columns)
    X_train, X_test, y_train, y_test = train_test_split(data_tt_att,
                                                        np.array(data_cor_att['runs_y'].tolist()),test_size=0.25,
                                                        random_state=123456
                                                        )
    print(X_train.shape,X_test.shape,y_train.shape,y_test.shape)

    train_dataset = tf.data.Dataset.from_tensor_slices((X_train,y_train))\
                                .shuffle(500)\
                                .batch(N_BATCH,drop_remainder=True)\
                                .repeat()
    test_dataset = tf.data.Dataset.from_tensor_slices((X_test,y_test)).batch(N_BATCH)
    with tf.device("/gpu:0"):
        model_att= create_model(15)
        model_att.compile(optimizer=tf.keras.optimizers.Adam(learning_rate=learnig_rate),loss='mse')

        print(model_att.summary())

        steps_epoch = X_train.shape[0]//N_BATCH
        validation_steps = int(np.ceil(X_test.shape[0]/N_BATCH))

        history = model_att.fit(train_dataset,epochs=N_EPOCHS,
                            steps_per_epoch=steps_epoch,
                            validation_data = test_dataset,
                            validation_steps=validation_steps)

        model_att.evaluate(test_dataset)
        print(model_att.evaluate(test_dataset))
        history.history.keys()
        print(history.history.keys())

        pred=model_att.predict(X_test)
        print(pred.flatten())
        print("test_labels =", y_test)
        # model_att.save('model_att.h5')
        
        # joblib.dump(model_att,'att1.h5')
        # new_model = joblib.load('att1.joblib')
        # defend model learning
        
        now=datetime.datetime.now()
        SaveModel(model_att,f'model_att','att',now)
    print("learning score_model")

def loss_model():
    client=MongoClient(host)
    db_def=client['mov_avg_defend_y']

    col_list=db_def.list_collection_names()
    list_def=[]

    learn_date=datetime.datetime(2023,1,1)

    query={
        "game_date":{
            '$lt':learn_date
        }
    }
    for coll in col_list:
        try:
            data_def=list(db_def[coll].find(query))
        except Exception as e:
            print(e)
        list_def.extend(data_def)
    client.close()
    data_all_def=pd.DataFrame(list_def).drop(columns='_id').dropna()
    print(data_all_def)

    print(data_all_def.describe())


    data_cor_def=data_all_def.drop(columns=['game_date','spID','game_id'])
    data_cor_def=IQR(data_cor_def)
    print(data_cor_def.corrwith(data_cor_def['runs_p_y']))



    data_cor_def=data_all_def.drop(columns=['game_date','spID','game_id'])
    data_cor_def=IQR(data_cor_def)
    print(data_cor_def.corrwith(data_cor_def['runs_p_y']))



    data_tt_def=data_cor_def.drop(columns=['runs_p_y'])

    # minmax scaling
    data_tt_def=(data_tt_def-data_tt_def.min())/(data_tt_def.max()-data_tt_def.min())
    print(data_tt_def)


    
    with tf.device("/gpu:0"):
        print(data_tt_def.columns)
        X_train, X_test, y_train, y_test = train_test_split(data_tt_def,
                                                            np.array(data_cor_def['runs_p_y'].tolist()),test_size=0.25,
                                                            random_state=123456
                                                            )
        print(X_train.shape,X_test.shape,y_train.shape,y_test.shape)

        train_dataset = tf.data.Dataset.from_tensor_slices((X_train,y_train))\
                                    .shuffle(500)\
                                    .batch(N_BATCH,drop_remainder=True)\
                                    .repeat()
        test_dataset = tf.data.Dataset.from_tensor_slices((X_test,y_test)).batch(N_BATCH)


        model_def= create_model(17)
        model_def.compile(optimizer=tf.keras.optimizers.Adam(learning_rate=learnig_rate),loss='mse')

        print(model_def.summary())

        steps_epoch = X_train.shape[0]//N_BATCH
        validation_steps = int(np.ceil(X_test.shape[0]/N_BATCH))

        history = model_def.fit(train_dataset,epochs=N_EPOCHS,
                            steps_per_epoch=steps_epoch,
                            validation_data = test_dataset,
                            validation_steps=validation_steps)

        model_def.evaluate(test_dataset)
        print(model_def.evaluate(test_dataset))
        history.history.keys()
        print(history.history.keys())




        plt.figure(figsize=(10,7))

        plt.plot(range(N_EPOCHS),history.history['loss'],label='train loss')
        plt.plot(range(N_EPOCHS),history.history['val_loss'],label='validation loss')

        plt.xlabel('Epoch')
        plt.ylabel('Loss')
        plt.legend()
        plt.show()

        pred=model_def.predict(X_test)
        print(pred.flatten())
        print("test_labels =", y_test)




        now=datetime.datetime.now()
        SaveModel(model_def,f'model_def','def',now)
    print("learning loss model")

def predict_win():
    now=datetime.now()
    today=str(now)[:10]
    match_day=datetime.strptime(today,'%Y-%m-%d')
    host = "mongodb://localhost:27017/"
    client=MongoClient(host)
    print(match_day)
    db_model = client['model']
    db_att = client['mov_avg_attack_y']
    db_def = client['mov_avg_defend_y']

    # get input data

    games=statsapi.schedule(date=today)
    if(len(games)==0):
        return
    list_score_home=[]
    list_score_away=[]
    list_loss_home=[]
    list_loss_away=[]
    for game in games:
        if(game['game_type']!='R'):
            continue
        if(game['status']!='Final'):
            continue
        game_id=game['game_id']
        box_dict=statsapi.boxscore_data(game_id)
        home_id=box_dict['teamInfo']['home']['abbreviation']
        away_id=box_dict['teamInfo']['away']['abbreviation']

        query={'game_id':game_id}
        # query={
        #     'game_date':{
        #         '$lt':match_day
        #     }
        # }
        try:
            score_home_data=list(db_att[home_id].find(query))
            score_away_data=list(db_att[away_id].find(query))
            loss_home_data=list(db_def[home_id].find(query))
            loss_away_data=list(db_def[away_id].find(query))
            score_home_data[0]['home']=home_id
            score_away_data[0]['away']=away_id
            list_score_home.extend(score_home_data)
            list_score_away.extend(score_away_data)
            list_loss_home.extend(loss_home_data)
            list_loss_away.extend(loss_away_data)
        except Exception as e:
            print(e)
    score_df_home=pd.DataFrame(list_score_home).drop_duplicates(['game_id'],keep='first')
    score_df_away=pd.DataFrame(list_score_away).drop_duplicates(['game_id'],keep='last')
    loss_df_home=pd.DataFrame(list_loss_home).drop_duplicates(['game_id'],keep='first')
    loss_df_away=pd.DataFrame(list_loss_away).drop_duplicates(['game_id'],keep='last')

    if('_id' not in score_df_home):
        return
    # print(score_df_home)
    # print(score_df_away)
    # print(loss_df_home)
    # print(loss_df_away)




    # attack
    with tf.device("/gpu:0"):
        collection_model = db_model['model_att']
        model_name = 'att'
        model_fpath = f'{model_name}.joblib'
        result=collection_model.find({"model_name": model_name}).sort([("inserted_time",-1)])
        print(result)
        cnt=len(list(result.clone()))
        # print(cnt)
        try:
            file_id = str(result[0]['file_id'])
            model_att=LoadModel(mongo_id=file_id).clf
        except Exception as e:
            print(e)
        # model_att=LoadfromMongo('att','model_att')
        # joblib.dump(model_att,model_fpath)

        print(model_att.summary())
        target_score_home=score_df_home.drop(columns=['_id','home','game_date','game_id','spID','runs_y'])
        target_score_away=score_df_away.drop(columns=['_id','away','game_date','game_id','spID','runs_y'])
        target_score_home=(target_score_home-target_score_home.min())/(target_score_home.max()-target_score_home.min())
        target_score_away=(target_score_away-target_score_away.min())/(target_score_away.max()-target_score_away.min())
        y_pred_score_home = model_att.predict(target_score_home)
        y_pred_score_away = model_att.predict(target_score_away)
        score_df_home['pred_score']=y_pred_score_home
        score_df_away['pred_score']=y_pred_score_away

        # defend
        collection_model = db_model['model_def']
        model_name = 'def'
        model_fpath = f'{model_name}.joblib'
        result=collection_model.find({"model_name": model_name}).sort([("inserted_time",-1)])
        print(result)
        cnt=len(list(result.clone()))
        print(cnt)
        try:
            file_id = str(result[0]['file_id'])
            model_def=LoadModel(mongo_id=file_id).clf
        except Exception as e:
            print(e)
            return 1
        joblib.dump(model_def,model_fpath)

        # print(model_def.summary())
        target_loss_home=loss_df_home.drop(columns=['_id','game_date','game_id','spID','runs_p_y'])
        target_loss_away=loss_df_away.drop(columns=['_id','game_date','game_id','spID','runs_p_y'])
        target_loss_home=(target_loss_home-target_loss_home.min())/(target_loss_home.max()-target_loss_home.min())
        target_loss_away=(target_loss_away-target_loss_away.min())/(target_loss_away.max()-target_loss_away.min())
        y_pred_loss_home = model_def.predict(target_loss_home)
        y_pred_loss_away = model_def.predict(target_loss_away)
        loss_df_home['pred_loss']=y_pred_loss_home
        loss_df_away['pred_loss']=y_pred_loss_away

        # print(score_df_home)
        # print(score_df_away)
        # print(loss_df_home)
        # print(loss_df_away)
        n_pytago=1.83
        home_win=pd.merge(score_df_home[['home','game_id','runs_y','pred_score']],loss_df_home[['game_date','game_id','spID','runs_p_y','pred_loss']],how='inner',on='game_id')
        home_win['win_rate']=(pow(home_win['pred_score'],n_pytago))/(pow(home_win['pred_score'],n_pytago)+pow(home_win['pred_loss'],n_pytago))
        # print(home_win)
        away_win=pd.merge(score_df_away[['away','game_id','runs_y','pred_score']],loss_df_away[['game_date','game_id','spID','runs_p_y','pred_loss']],how='inner',on='game_id')
        away_win['win_rate']=(pow(away_win['pred_score'],n_pytago))/(pow(away_win['pred_score'],n_pytago)+pow(away_win['pred_loss'],n_pytago))
        # print(away_win)
        win=pd.merge(home_win,away_win,how='inner',on='game_id')
        win['winner']=win['win_rate_x']-win['win_rate_y']
        win['real_winner']=win['runs_y_x']-win['runs_y_y']
        # cond = win['winner']>=0
        # df_win = win[cond]
        # print(df_win['winner'].count(),win['winner'].count()-df_win['winner'].count())

        # print(win)
        data=win.to_dict('records')
        db_result=client['predict_result']
        colllection_result=db_result['result']
        colllection_result.create_index([("game_id",ASCENDING)],unique=True)
        try:
            colllection_result.insert_many(data,ordered=False)
        except Exception as e:
            print(e)
    client.close()
    print("prediction")

if __name__ == "__main__":
    print("entering main")
    print(sys.argv[0])
    if sys.argv[1] == 'score_model':
        print("entering score_model")
        score_model()
    elif sys.argv[1] == 'loss_model':
        print("entering loss")
        loss_model()
    elif sys.argv[1] == 'predict_win':
        print("entering predict")
        predict_win()
    
    print("hello main")
 
