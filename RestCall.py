import requests
import pymongo
import sys
import json

mech_intern_collection = 'careeronemechanicalintern'
mech_intern = 'mechanical intern'
eee_intern_collection = 'careeroneelectricalintern'
eee_intern = 'electrical intern'
civil_intern_collection = 'careeronecivilintern'
civil_intern = 'civil intern'
sw_intern_collection = 'careeronesoftwareintern'
sw_intern = 'software intern'
hw_intern_collection = 'careeronehardwareintern'
hw_intern = 'hardware intern'
mech_engineer_collection = 'careeronemechanicalengg'
mech_engineer = 'mechanical engineer'
eee_engineer_collection = 'careeroneelectricalengg'
eee_engineer = 'electrical engineer'
civil_engineer_collection = 'careeronecivilengg'
civil_engineer = 'civil engineer'
sw_engineer_collection = 'careeronesoftwareengg'
sw_engineer = 'software engineer'
hw_engineer_collection = 'careeronehardwareengg'
hw_engineer = 'hardware engineer'


def job_execute(collection_name, key_word):
    connection = pymongo.MongoClient('mongodb://127.0.0.1:27017')
    db = connection['jobsdashboard']
    collection = db[collection_name]
    api_key = 'Bearer DV/cslqt7eHZehYbYaaf8XYeuRb6UfqVDactyyppfphbcEbTDE2Ybt55vYlwEF75xNZG1PV7zvaT7jiBIGMdnA=='
    url = 'https://api.careeronestop.org/v1/jobsearch/mxcsgJWpVblCWwn/' + key_word + '/USA/25/0/0/0/1/6'
    headers = {'Authorization': api_key, 'Content-Type': 'application/json'}
    resp = requests.get(url, headers=headers).json()
    counter = 0
    list = []
    jvlist = []
    print(resp)
    dict = resp
    jobcount=0
    jobcount = int(dict["Jobcount"])
    try:
        val = collection.find()
        print('number of jobs in ' + key_word + ' db ' + str(val.count()))
        print('number of jobs in ' + key_word + ' portal ' + str(jobcount))
        index = 0
        if val.count() == 0:
            if jobcount > 500:
                print('in while')
                while jobcount > 0:
                    url = 'https://api.careeronestop.org/v1/jobsearch/mxcsgJWpVblCWwn/' + key_word + '/USA/25/0/0/' + str(index) + '/' + str(jobcount) + '/6'
                    headers = {'Authorization': api_key, 'Content-Type': 'application/json'}
                    resp = requests.get(url, headers=headers).json()
                    dict = resp
                    list.append(dict["Jobs"])
                    index += 500
                    jobcount -= 500
            else:
                url = 'https://api.careeronestop.org/v1/jobsearch/mxcsgJWpVblCWwn/' + key_word + '/USA/25/0/0/0/' + str(jobcount) + '/6'
                headers = {'Authorization': api_key, 'Content-Type': 'application/json'}
                resp = requests.get(url, headers=headers).json()
                dict = resp
                list.append(dict["Jobs"])
            for job in list:
                    print(job)
                    collection.insert(job)
        elif jobcount > int(val.count()):
            print('mismatch')
            url = 'https://api.careeronestop.org/v1/jobsearch/mxcsgJWpVblCWwn/' + key_word + '/USA/25/0/0/'+ str(val.count()) +'/' + str(jobcount) + '/6'
            headers = {'Authorization': api_key, 'Content-Type': 'application/json'}
            resp = requests.get(url, headers=headers).json()
            dict = resp
            list = dict["Jobs"]
            print(len(list))
            #result=collection.insert_many(list)
            #result.inserted_ids
            for job in list:
                jvlist.append(job["JvId"])
            for job in list:
               if job["JvId"] not in jvlist:
                    collection.insert(job)
                    print(job)

    except Exception as e:
        print('Unexpected Exception: ', type(e), e)
    connection.close()


job_execute(mech_intern_collection, mech_intern)
job_execute(eee_intern_collection, eee_intern)
job_execute(civil_intern_collection, civil_intern)
job_execute(sw_intern_collection, sw_intern)
job_execute(hw_intern_collection, hw_intern)
job_execute(mech_engineer_collection, mech_engineer)
job_execute(eee_engineer_collection, eee_engineer)
job_execute(civil_engineer_collection, civil_engineer)
job_execute(sw_engineer_collection, sw_engineer)
job_execute(hw_engineer_collection, hw_engineer)