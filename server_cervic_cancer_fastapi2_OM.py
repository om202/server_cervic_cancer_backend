# TO run the script
# python cervic_cancer_fastapi1.py


import pymongo
from bson.objectid import ObjectId
from bson.json_util import dumps, RELAXED_JSON_OPTIONS
import re
import requests
from datetime import datetime, timedelta
from dateutil.parser import parse
from typing import Optional
from fastapi import FastAPI
from pydantic import Field
import uvicorn
import redis
import uuid
import json
import threading
import dateutil.parser
from typing import List, Tuple, Dict
import logging

import aioredis
from starlette.middleware.cors import CORSMiddleware
from starlette.websockets import WebSocket, WebSocketDisconnect
from websockets.exceptions import ConnectionClosed

# initialize constants used for server queuing
IMAGE_QUEUE = "image_queue"

''' Initialise FAST API '''
app = FastAPI()

''' Initialise  Mongo DB'''
#my_client = pymongo.MongoClient("mongodb://localhost:27017/")

# my_client = pymongo.MongoClient("mongodb://cervicuser:12345@localhost:27017/?authSource=cervic_database")

my_client = pymongo.MongoClient("mongodb://10.128.0.9:27017/")

# initialize constants used for server queuing

# IMAGE_QUEUE = "image_queue"
# RESULT_QUEUE = "result_queue"
BATCH_SIZE = 8
SERVER_SLEEP = 0.25
CLIENT_SLEEP = 0.25

# app.config['JSON_SORT_KEYS'] = False


#db = redis.StrictRedis(host="localhost", port=6379, db=0)


db = redis.StrictRedis(host= "10.128.0.9", port=6379, db=0)

data = {"Test_ID": None, "predictions_out": None, "write_db": False}
result_data = {"output": None}
result_data1 = {"output": None}

list_threads = []
listofclients = []

#
# Instantiate the logger and FastAPI app
#
logger = logging.getLogger("main")

# Allow CORS for requests coming from any domain
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"]
)

# stream_name, message_id, dict of key->value pairs in message
Message = Tuple[bytes, bytes, Dict[bytes, bytes]]

# my_client = pymongo.MongoClient("mongodb://cervicuser:12345@35.232.108.197:27017/?authSource=cervic_database")



# @app.get('/test_append_api')
def test_append_api(hosp_pres: Optional[str] = None, test_id: Optional[str] = None):
    """

    The Cervical cancer server does analysis and creates documents. These results are stored in database.

    The TEST APPEND API is to append the new document into MongoDB database.

   -  :param **hosp_pres** : should have hosp_pres

              If hosp_pres is None , App_Status = 1400

   -  :param **test_id** : should have test_id

             If test_id requested for TEST APPEND API is None, App_Status  = 1405

             If test_id requested for TEST APPEND API is not ready in cervical cancer server, App_Status = 1451

             If test_id requested for TEST APPEND API is already stored in database, App_Status = 1252

   -  :return: Appropriate messages based on SUCCESS or Error conditions

   ***If test_append_api is SUCCESS, the NEW TEST results are stored in database and App_Status= 1251***

    """
    hosp_pres_id = hosp_pres
    testid = test_id
    if hosp_pres_id is None:
        return (
            {'App_Status': 1400,
             'Message': 'Bad request. Hospital prescribed id is not specified for TEST APPEND API.'})
    if testid is None:
        return {'App_Status': 1405, 'Message': 'Bad request. TEST ID is not specified for TEST APPEND API...'}
    print('Test id requested for append :' + testid)
    db_list = my_client.list_database_names()
    if "cervic_database" in db_list:
        print("The cervical_database exists.")
    my_cervic_db = my_client["cervic_database"]
    collection_list = my_cervic_db.list_collection_names()
    my_collection = my_cervic_db[hosp_pres_id]
    test_ids_list_res = my_collection.distinct('Test_id')
    ''' If test_id requested is already in database then no need to append in database'''
    if testid in test_ids_list_res:
        return ({'App_Status': 1252, 'Test_id': testid,
                 'Message': 'The TEST_ID : ' + testid + ' which is requested is available in database.'})

    ''' Check the cervic cancer classifier endpoint is ready with results otherwise results are not ready'''

    #PARAMS = {"test_id": testid}
    #URL = 'http://127.0.0.1:3001/cervic_cancer_classifier_get'
    #response = requests.get(url=URL, params=PARAMS).content


    #new_data = json.loads(response)
    new_data = cervic_cancer_classifier_get(testid)
    
    app_status = new_data["App_Status"]
    print(app_status)
    collection_name_required = hosp_pres_id
    '''If the App_Status =1260, the data is ready in server '''
    if app_status == 1260:
        new_data_out = new_data["Output"]
        test_no = new_data_out["Test_ID"]
        print('Test results arrived from server :' + test_no)

        data_prediction = new_data_out["predictions_out"]
        collection_name = data_prediction["hosp_pres"]
        write_res = new_data_out["write_db"]

        if write_res is True and testid == test_no and collection_name_required == collection_name:
            my_collection = my_cervic_db[collection_name]
            cervix_record = data_prediction
            #print(cervix_record)
            value = cervix_record['_id']
            value_new = ObjectId(value)
            cervix_record['_id'] = value_new

            cervix_record['Test_id'] = test_no  # Made test_id same as received from client
            date_old = cervix_record['test_datetime']
            date_new = parse(date_old)
            cervix_record['test_datetime'] = date_new
            rec = my_collection.insert_one(cervix_record)
            print('one record inserted')
            message = 'TestAppendAPI is Success.' + ' TEST_ID :' + testid + '  is appended in Database in  ' + collection_name
            print('TEST_ID :' + testid + ' ' + message)
            return {'App_Status': 1251, 'Test_id': testid, 'Message': message}
        else:
            print('TEST_ID :' + testid + ' Results are not ready.')
            return {'App_Status': 1253, 'Test_id': testid, 'Message': 'TEST_ID' + testid + ' Results are not ready. '}
    elif app_status == 1261 and testid not in collection_list:
        print('TEST_ID : ' + testid + ' Results are not available in server.  ')
        return {'App_Status': 1253, 'Test_id': testid, 'Message': 'TEST_ID :' + testid + 'Results are not available in '
                                                                                        'server.'}

async def read_from_stream(
    redis: aioredis.Redis, stream: str, latest_id: str = None, past_ms: int = None, last_n: int = None
) -> List[Message]:
    timeout_ms = 60 * 10000

    # Blocking read for every message added after latest_id, using XREAD
    if latest_id is not None:
        return await redis.xread([stream], latest_ids=[latest_id], timeout=timeout_ms)

    # Blocking read for every message added after current timestamp minus past_ms, using XREAD
    if past_ms is not None:
        server_time_s = await redis.time()
        latest_id = str(round(server_time_s * 1000 - past_ms))
        return await redis.xread([stream], latest_ids=[latest_id], timeout=timeout_ms)

    # Non-blocking read for last_n messages, using XREVRANGE
    if last_n is not None:
        messages = await redis.xrevrange(stream, count=last_n)
        return list(reversed([(stream.encode("utf-8"), *m) for m in messages]))

    # Default case, blocking read for all messages added after calling XREAD
    return await redis.xread([stream], timeout=timeout_ms)


@app.websocket("/stream/{stream}")
async def proxy_stream(
    ws: WebSocket,
    stream: str,
    latest_id: str = None,
    past_ms: int = None,
    last_n: int = None,
    max_frequency: float = None,
):
    await ws.accept()
    # Create redis connection with aioredis.create_redis
    redis = await aioredis.create_redis_pool("redis://10.128.0.9:6379", db=0)

    # Loop for as long as client is connected and our reads don't time out, sending messages to client over websocket
    while True:
        # Limit max_frequency of messages read by constructing our own latest_id
        to_read_id = latest_id
        if max_frequency is not None and latest_id is not None:
            ms_to_wait = 10000 / (max_frequency or 1)
            ts = int(latest_id.split("-")[0])
            to_read_id = f"{ts + max(0, round(ms_to_wait))}"

        # Call read_from_stream, and return if it raises an exception
        messages: List[Message]
        try:
            messages = await read_from_stream(redis, stream, to_read_id, past_ms, last_n)
        except Exception as e:
            logger.info(f"read timed out for stream {stream}, {e}")
            return
        
        print("print messages", messages)
        # If we have no new messages, note that read timed out and return
        if len(messages) == 0:
            logger.info(f"no new messages, read timed out for stream {stream}")
            return

        # If we have max_frequency, assign only most recent message to messages
        if max_frequency is not None:
            messages = messages[-1:]

        # Prepare messages (message_id and JSON-serializable payload dict)
        prepared_messages = []
        for msg in messages:
            latest_id = msg[1].decode("utf-8")
            payload = {k.decode("utf-8"): v.decode("utf-8") for k, v in msg[2].items()}
            prepared_messages.append({"message_id": latest_id, "payload": payload})

        # Send messages to client, handling (ConnectionClosed, WebSocketDisconnect) in case client has disconnected
        try:
            await ws.send_json(prepared_messages)
        except (ConnectionClosed, WebSocketDisconnect):
            logger.info(f"{ws} disconnected from stream {stream}")
            return

def cervic_classifier_process(image_list_all, filenamelist_all, dispcell, test_id, test_type, stain_type , patient_id, patient_age,  test_pres_doctor, dept_pres, hosp_pres, roi_taken, test_taken, test_datetime):
    print('Classifier process started for Test ID : ' + str(test_id))
    # print(dispcell)
    # print(type(dispcell))
    displaycell_flag = int(dispcell)
    # print(test_id)
    data["Test_ID"] = test_id
    data["predictions_out"] = None
    data["write_db"] = False

    kk = str(uuid.uuid4())

    for imgID in range(len(image_list_all)):
        img = image_list_all[imgID]
        filenamelist = filenamelist_all[imgID]
        k = str(uuid.uuid4())

        d = {"id": k, "Test_id": test_id, "Test_type": test_type, "Stain_type": stain_type, "image": img, "displaycell": displaycell_flag,
             "image_id": 'image_' + str(imgID), "image_name": filenamelist, "Patient_id": patient_id, "Patient_age": patient_age,
             "Test_pres_doctor": test_pres_doctor, "Dept_pres": dept_pres, "Hosp_pres": hosp_pres,
             "Roi_taken": roi_taken, "Test_taken": test_taken, "Test_datetime": test_datetime}

        db.rpush(IMAGE_QUEUE, json.dumps(d, default=str))

    while True:

        output = db.get(k)

        # check to see if model has classified the input

        if output is not None:
            # add the output predictions to our data dictionary

            output = output.decode("utf-8")
            data["predictions_out"] = json.loads(output)

            # delete the result from the database and break from the polling loop
            db.delete(k)
            break

    # sleep for a small amount to give the model a chance to classify the input image

    # time.sleep(CLIENT_SLEEP)
    data["write_db"] = True
    print('Write_db value is :' + str(data["write_db"]))
    print('Finished Cervical cancer Analysis')
    print('Writing the new test results in Redis')

    kk_new = test_id
    db.set(kk_new, json.dumps(data))
    # db.set('Test_id_comp', test_id)
    print('Test_id' + kk_new + ' is stored in redis')
    testappendresult = test_append_api(hosp_pres, test_id)
    testappendstatus= testappendresult['App_Status']
    if testappendstatus == 1251:
      print('Test Appended Successfully '  + testappendresult['Message']) 
    elif testappendstatus == 1252:
  
      print('Test id already stored in database ' + testappendresult['Test_id']+ testappendresult['Message']) 
    elif testappendstatus == 1253: 
  
      print('Results are not available in server ' + testappendresult['Test_id']+ testappendresult['Message']) 
    db.delete(test_id)
    stream_name = 'cervic'
    
    results = {'test_id': test_id, 'commit_status': 1}
    print('stream name : '+ stream_name)
    print('stream results : ')
    print(results)
    id1 = db.xadd(stream_name, results, id= u'*', maxlen=None, approximate=True)
    print(id1)
    print('Reddis Stream added')
  
@app.get('/remove_test')
def remove_test(hosp_pres: Optional[str] = None, test_id: Optional[str] = None):
    """
    REMOVES A PARTICULAR TEST

    - :param **hosp_pres** : should have hosp_pres

            If hosp_pres is None, App_Status = 1400

            If hosp_pres is not available, App_Status = 1100

    - :param **test_id** : should have test_id

            If test_id is None, App_Status = 1405

            If test_id is not available, App_Status = 1105

    - :return: All information for a given a TEST ID

    ***If SUCCESS, App_Status = 1205***

    """
    hosp_pres_id = hosp_pres
    test_id_req = test_id
    if hosp_pres_id is None:
        return ({'App_Status': 1400,
                 'Message': 'Bad request. Hospital prescribed id is not specified for SEARCH by TEST ID  display...'})
    if test_id_req is None:
        return {'App_Status': 1405,
                'Message': 'Bad request. TEST id is not specified for  SEARCH by TEST ID  display...'}
   
    db_list = my_client.list_database_names()

    if "cervic_database" in db_list:
        print("The cervical_database exists.")
    my_cervic_db = my_client["cervic_database"]
    collection_list = my_cervic_db.list_collection_names()
    if hosp_pres_id not in collection_list:
        return ({'App_Status': 1100,
                 'Message': 'Bad request. Hospital prescribed id requested for SEARCH by TEST ID  display is not in '
                            'database ...'})

    collection_name = hosp_pres_id
    mycollection = my_cervic_db[collection_name]
    del_q = {"Test_id": f"{test_id_req}"}
    test_id_meta_res = mycollection.remove(del_q)
   
    if(test_id_meta_res['n'] == 1):
        done = {"removed": "true"}
    else:
        done = {"removed": "false"}

    return done

#@app.get('/cervic_cancer_classifier_get')
def cervic_cancer_classifier_get(test_id: str):
    """
    The cervic_cancer_classifier_get is used by TEST APPEND API to check whether the results are available or not.

    - :param **test_id** : should have test_id
    - If the given test_id results are available in server , App_Status = 1260

    - If the given test_id results are not available in server, App_Status = 1261

    - :return: Cervical cancer Analysis results if it is available otherwise error message.

    """
    test_reqd = test_id

    ''' 
    Check whether the redis queue has the cervical analysis results of a given TEST ID
    '''
    testoutput1 = db.get(test_reqd)

    # check to see if test_id is completed

    if testoutput1 is not None:
        # add the output predictions to our data dictionary

        testoutput1 = testoutput1.decode("utf-8")
        result_data1["output"] = json.loads(testoutput1)

        ''' Store the cervical analysis results in results_new variable. '''

        results_new = result_data1["output"]
        write_res = results_new["write_db"]

        test_arrived = results_new["Test_ID"]

        ''' Check whether the redis queue has data and whether TEST ID required by client and 
        result available in server are same'''
        if write_res is True and test_reqd == test_arrived:
            print('Test_id :' + test_arrived + ' results are sent to client.')
            db.delete(test_reqd)

            return ({'App_Status': 1260, 'Test_id': test_arrived, 'Output': results_new,
                     'Message': ' TEST_ID :' + test_arrived + ' results are sent to Database API.'})

    else:
        print(' Server response:  TEST_ID : ' + test_reqd + ' Results are not available.')
        return ({'App_Status': 1261, 'Test_id': test_reqd,
                 'Message': ' Server response: TEST_ID : ' + test_reqd + ' Results are not available. '})


@app.post('/cervic_cancer_classifier')
async def cervic_cancer_classifier(input_json: dict):
    """
    The cervic_cancer_classifier is NEW TEST API.

    - :param **input_json** : Contains JSON Of the following
    - :param **dispcell** : To display cell details or Not

            If dispcell is None, App_Status = 1451

    - :param **test_id** : TEST_ID i.e. Test1

            If test_id is None, App_Status = 1452

    - :param **test_type** : TEST_ID i.e. Pap Smear or LBC

            If test_type is None, App_Status = 1452

    - :param **stain_type** : TEST_ID i.e. Papanicolaou Stain (Pap Smear)

            If stain_type is None, App_Status = 1452


    - :param ** patient_id** : PATIENT ID i.e. P01

            If patient_id is None, App_Status = 1453

    - :param ** patient_age** : PATIENT AGE i.e. integer 

            If patient_age is None, App_Status = 1453


    - :param **test_pres_doctor** : TEST PRESCRIBED DOCTOR i.e MALHOTRA

            If test_pres_doctor is None, App_Status = 1454

    - :param **dept_pres** : DEPARTMENT PRESCRIBED. i.e doctor name

            If dept_pres is None, App_Status = 1455

    - :param **hosp_pres** : HOSPITAL PRESCRIBED ID i.e RCC

            If hosp_pres is None, App_Status = 1456

    - :param **roi_taken** : ROI TAKEN i.e doctor who is in-charge of ROI

            If roi_taken is None, App_Status = 1457

    - :param **test_taken** : TEST TAKEN i.e doctor who is in-charge of TEST TAKEN

            If test_taken is None, App_Status = 1458

    - :param **test_datetime** : TEST DATETIME i.e TEST Datetime

           If test_datetime is None, App_Status = 1459

    - :param **image_list** : Image list which contains list of images in encoded base64 format.

           If image_list is None, App_Status= 1461

           If image_list is empty list, App_Status = 1462

    - :param **filename_list** : Filename list is the names of images submitted by client.

          If filename_list is None, App_Status = 1460

    - :return: A message showing Uploaded images successfully or appropriate error conditions

          If upload is successful, App_Status  =1250

          If the server is busy, when the client submits NEW TEST, App_Status = 1150

    ***If cervic_cancer_classifier is SUCCESS, App_Status = 1250***

    """
    incoming_data = input_json
    dispcell = incoming_data["dispcell"]
    test_iden = incoming_data["test_id"]
    test_type = incoming_data["test_type"]
    stain_type = incoming_data["stain_type"]
    patient_iden = incoming_data["patient_id"]
    patient_age = incoming_data["patient_age"]
    print('Json contents received from Client ')
    #print('dispcell : '+dispcell)
    print('test_id : '+test_iden)
    
    print('test_type : '+test_type)
    print('stain_type : '+stain_type)
    print('patient_id : '+patient_iden)
    print('patient_age : '+patient_age)

    test_pres_doc = incoming_data["test_pres_doctor"]

    dept_pres_by = incoming_data["dept_pres"]
    hosp_pres_by = incoming_data["hosp_pres"]
    roi_taken_by = incoming_data["roi_taken"]
    test_taken_by = incoming_data["test_taken"]
    test_datetime_by = incoming_data["test_datetime"]
    print('test_pres_doctor : '+test_pres_doc)
    print('dept_pres : '+dept_pres_by)
    print('hosp_pres : '+hosp_pres_by)
    print('roi_taken : '+roi_taken_by)
    print('test_taken : '+test_taken_by)
    print('test_datetime : '+test_datetime_by)

    if dispcell is None:
        return {'App_Status': 1451, 'Message': 'Bad request. Display cell image option is not specified.'}
    if test_iden is None:
        return {'App_Status': 1452, 'Message': 'Bad request. Test identifier is not specified for new test.'}
    if test_type is None:
        return {'App_Status': 1452, 'Message': 'Bad request. Test type is not specified for new test.'}

    if stain_type is None:
        return {'App_Status': 1452, 'Message': 'Bad request. Stain type is not specified for new test.'}

    if patient_iden is None:
        return {'App_Status': 1453, 'Message': 'Bad request. Patient identifier is not specified for new test.'}

    if patient_age is None:
        return {'App_Status': 1453, 'Message': 'Bad request. Patient age is not specified for new test.'}

    if test_pres_doc is None:
        return (
            {'App_Status': 1454,
             'Message': 'Bad request. Test prescribed by doctor name is not specified for new test.'})
    if dept_pres_by is None:
        return (
            {'App_Status': 1455,
             'Message': 'Bad request. Department prescribed for test is not specified for new test.'})

    if hosp_pres_by is None:
        return (
            {'App_Status': 1456, 'Message': 'Bad request. Hospital prescribed for test is not specified for new test.'})

    if roi_taken_by is None:
        return {'App_Status': 1457, 'Message': 'Bad request. ROI taken by name is not specified for new test.'}

    if test_taken_by is None:
        return {'App_Status': 1458, 'Message': 'Bad request. Test taken by name is not specified for new test.'}

    if test_datetime_by is None:
        return {'App_Status': 1459, 'Message': 'Bad request. Test taken date and time is not specified for new test.'}

    filenamelist_all = incoming_data["filename_list"]
    if filenamelist_all is None:
        return {'App_Status': 1460, 'Message': 'Bad request. Image filenames are specified for new test.'}

    image_list = incoming_data["image_list"]

    if image_list is None:
        return {'App_Status': 1461, 'Message': 'Bad request. Images are not uploaded for new test.'}

    if len(image_list) == 0:
        return {'App_Status': 1462, 'Message': 'Bad request. Image list is empty for new test.'}

    global listofclients

    global list_threads

    d = dateutil.parser.parse(test_datetime_by).isoformat() + 'Z'

    if len(list_threads) == 0 and len(listofclients) == 0:  # first client test submission
        for thread_no in range(len(listofclients) + 1):
            t = threading.Thread(target=cervic_classifier_process,
                                 kwargs=({'image_list_all': image_list, 'filenamelist_all': filenamelist_all,
                                          'dispcell': dispcell, 'test_id': incoming_data["test_id"], 
                                          'test_type': incoming_data["test_type"],
                                          'stain_type': incoming_data["stain_type"],
                                          'patient_id': incoming_data["patient_id"],
                                          'patient_age': incoming_data["patient_age"],                                          
                                          'test_pres_doctor': incoming_data["test_pres_doctor"],
                                          'dept_pres': incoming_data["dept_pres"],
                                          'hosp_pres': incoming_data["hosp_pres"],
                                          'roi_taken': incoming_data["roi_taken"],
                                          'test_taken': incoming_data["test_taken"],
                                          'test_datetime': d
                                          }))

            list_threads.append(t)
            listofclients.append(test_iden)

        if not (list_threads[0].isAlive()):  # first thread is not running so start it

            test_iden = incoming_data["test_id"]
            message = {'App_Status': 1250, 'Test_id': test_iden,
                       'Message': 'TEST_ID :' + test_iden + 'Images are uploaded successfully. Please wait for '
                                                            'results...',
                       'write_db': False}
            list_threads[0].start()  # start the first thread
            # print('THREAD STATUS AFTER START :' +str(list_threads[0].isAlive()))

            print('CLIENTS LIST AFTER FIRST START IS :' + str(listofclients))
            return message

    elif len(listofclients) > 0 and list_threads[
        0].isAlive():  # second client test submission while first client is running
        print('Processing is going on. Please wait till the current work is completed.')
        return ({'App_Status': 1150,
                 'Message': 'Processing is going on. Could not process ' + test_iden + '. Please wait till the '
                                                                                       'current work is completed.'})

    elif len(list_threads) > 0 and len(listofclients) > 0 and not list_threads[
        0].isAlive():  # first test is completed and new request is coming then accept it.

        list_threads = []  # empty the threads
        listofclients = []  # empty the clients
        for thread_no in range(len(listofclients) + 1):
            t = threading.Thread(target=cervic_classifier_process,
                                 kwargs=({'image_list_all': image_list, 'filenamelist_all': filenamelist_all,
                                          'dispcell': dispcell, 'test_id': incoming_data["test_id"],
                                          'test_type': incoming_data["test_type"],
                                          'stain_type': incoming_data["stain_type"],
                                          'patient_id': incoming_data["patient_id"],
                                          'patient_age': incoming_data["patient_age"],                                          
                                          'test_pres_doctor': incoming_data["test_pres_doctor"],
                                          'dept_pres': incoming_data["dept_pres"],
                                          'hosp_pres': incoming_data["hosp_pres"],
                                          'roi_taken': incoming_data["roi_taken"],
                                          'test_taken': incoming_data["test_taken"],
                                          'test_datetime': d
                                          }))
            list_threads.append(t)
            listofclients.append(test_iden)
        if not (list_threads[0].isAlive()):  # first thread is not running so start it

            test_iden = incoming_data["test_id"]
            message = {'App_Status': 1250, 'Test_id': test_iden,
                       'Message': 'TEST_ID :' + test_iden + 'Images are uploaded successfully. Please wait for '
                                                            'results...',
                       'write_db': False}
            list_threads[0].start()  # start the first thread

            print('CLIENTS LIST AFTER FIRST START IS :' + str(listofclients))
            return message


# if this is the main thread of execution first load the model and then start the server
#if __name__ == "__main__":
#    print("* Starting Database service...")
#    uvicorn.run('server_cervic_cancer_fastapi2_OM:app', host='0.0.0.0', port=3001, reload = True)
