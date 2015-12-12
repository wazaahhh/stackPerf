from matplotlib import use, get_backend
if 'Agg' != get_backend().title():
    use('Agg')


import urllib2

import numpy as np
import os,sys
import re
from datetime import datetime
import time
#import MySQLdb
import json
import csv
import shutil

import gzip,zlib
import json
import boto

import pandas

#sys.path.append("/home/ubuntu/toyCode/sLibs/")
#from tm_python_lib import *


class S3crawl():
    def __init__(self):
        self.iconn = None
        self.sconn = None

    def connectBucket(self,bucketName):
        s3 = boto.connect_s3()
        bucket = s3.get_bucket(bucketName)
        return bucket

    def retrieveJson(self,url,keyS3,overwrite=False):
        ''' check if file already exists '''
        if bucket.get_key(keyS3)!=None and overwrite==False:
            print "%s : key already exists on S3"%keyS3
            already_exists = True

        else:

            already_exists = False

            try:
                '''retrieve  gzipped json'''
                req = urllib2.Request(url)
                response = urllib2.urlopen(req)
                gzipJson= response.read()
                print "successfully retrieved gzipped JSON"
            except:
                print "error retrieving JSON (%s)"%url

            try:
                key = bucket.new_key(keyS3)
                key.set_contents_from_string(gzipJson)
                print "successfully saved gzipped JSON on S3"
            except:
                print "error uploading gzip file on S3"
                print keyS3
        try:
            '''check remaining quota'''
            key = bucket.get_key(keyS3)
            key.get_contents_to_filename('tmp/archive.gz')
            f = gzip.open('tmp/archive.gz', 'rb')
            J = f.read()
            f.close()

            J = json.loads(J)

            try:
                quota = J['quota_remaining']
                has_more = J['has_more']
                print "remaining quota : %s" % quota
            except:
                print J
                quota = 'error'
                has_more = 'error'

        except:
            print "error retrieving JSON and quota"
            J = 'error'
            quota = 'error'
            has_more = 'error'


        return J,has_more,quota,already_exists


    def listWebsites(self):

        url = "http://api.stackexchange.com/2.1/sites?key=%s&pagesize=999&filter=!-rLKUaOi"%StackOkey
        J,has_more,quota,already_exists = self.retrieveJson(url,"/main/sites.json",overwrite=True)

        print "\n\n"

        lastQsites = {}

        for i in range(len(J['items'])):

            lastQ = 0
            almostLastPage = 0

            api_site_name = J['items'][i]['api_site_parameter']
            print '\n',api_site_name,'\n'

            lastQ = self.lastQuestion(api_site_name)

            if int(lastQ)!=0:

                almostLastPage = self.almostLastPage(api_site_name)[1]
                lastQsites[api_site_name] = {"lastQuestion": lastQ, "lastPage" : almostLastPage}


            print '\n',i,api_site_name,lastQ,almostLastPage
            time.sleep(10)

        keyS3 = bucket.new_key("main/sites.json")
        keyS3.set_contents_from_string(json.dumps(lastQsites))
        print "successfully saved gzipped JSON on S3"

        return lastQsites

    def lastQuestion(self,api_site_name):

        date = datetime.now().date().strftime("%s")
        url ="http://api.stackexchange.com/2.1/questions?key=%s&pagesize=1&fromdate=%s&order=desc&sort=creation&site=%s"%(StackOkey,date,api_site_name)


        J,has_more,quota,already_exists = self.retrieveJson(url,"/tmp/testlastquestion_%s.gz"%api_site_name,overwrite=True)
        try:
            questionId = J['items'][0]['question_id']
        except:
            questionId = 0

        return questionId


    def almostLastPage(self,api_site_name):
        '''finds an approximation of the last page'''

        questionID = 0
        while questionID==0:
            questionID = self.lastQuestion(api_site_name)
            time.sleep(3)

        #print questionID
        #last page approximation

        ratios = np.arange(1.5,5,0.25)[::-1]
        for r in ratios:
            l = -1
            pageId = max(1,int(questionID/pageSize/r))
            print pageId,questionID,pageSize,r
            url = '''https://api.stackexchange.com/2.1/questions?key=%s&page=%s&pagesize=%s&order=asc&sort=creation&site=%s''' %(StackOkey,pageId,pageSize,api_site_name)
            #print url
            #filename = "%s/questions/creation_asc_page%s_psize%s.gz"%(data_dir,pageId,pageSize)

            while l<1:
                J,has_more,quota,already_exists = self.retrieveJson(url,"/tmp/lastPage.gz",overwrite=True)

                try:
                    l = len(J['items'])

                except:
                    #print J
                    l = -1
                    time.sleep(5)

            if l>0:
                print "done, last page : %s"%pageId

                time.sleep(5)
                break


            time.sleep(3)

        return J,pageId


    def getIds(self,keyS3):
        '''retrieves question timeline ids from a question page'''

        #key = bucket.get_key(keyS3)
        keyS3.get_contents_to_filename('tmp/archive.gz')
        f = gzip.open('tmp/archive.gz', 'rb')
        J = f.read()
        f.close()
        J = json.loads(J)

        questionIds = []

        for i in range(pageSize):
            qId = J['items'][i]['question_id']
            questionIds = np.append(questionIds,qId)

        return {'J':J,'questionIds' : map(int,questionIds)}


    def questions(self,api_site_name):
        '''randomly retrieve a question page'''
        R = range(1,sites[api_site_name]["lastPage"]+1)

        np.random.shuffle(R)

        for pageId in R[:1]:
            url = '''https://api.stackexchange.com/2.1/questions?key=%s&page=%s&pagesize=%s&order=asc&sort=creation&site=%s''' %(StackOkey,pageId,pageSize,api_site_name)
            #url = '''https://api.stackexchange.com/2.1/questions?page=%s&pagesize=%s&order=asc&sort=creation&site=stackoverflow''' %(pageId,pageSize)
            print url

            keyS3 = "%s/questions/creation_asc_page%s_psize%s.gz"%(api_site_name,pageId,pageSize)
            J,has_more,quota,already_exists = self.retrieveJson(url,keyS3,overwrite=False)

            if not already_exists:
                key = bucket.get_key(keyS3)
                keyList[api_site_name] = np.append(keyList[api_site_name],key)

        return J,quota


    def timeline(self,api_site_name,id):
        '''retrieve question timeline id'''
        id = int(id)

        page = 1

        has_more = True

        while has_more == True:
            url = '''https://api.stackexchange.com/2.1/questions/%s/timeline?key=%s&page=%s&pagesize=%s&site=%s'''%(id,StackOkey,page,pageSize,api_site_name)
            print url

            keyS3 = "%s/timelines/%s/page%s_pageSize%s.gz"%(api_site_name,id,page,pageSize)

            J,has_more,quota,already_exists = self.retrieveJson(url,keyS3,overwrite=False)
            print "has_more pages: %s"%has_more
            page +=1


        return J,quota,already_exists


    def makeKeylistSites(self,sites,type="questions",reload=False,save=True):


        if not reload:
            key = bucket.get_key("keyList.json.zlib")
            keyList = json.loads(zlib.decompress(key.get_contents_as_string()))
            return keyList


        keyList = {}


        for api_site_name in sites.keys():
            #print api_site_name

            keyList[api_site_name] = np.array([])
            keys = bucket.list("%s/%s"%(api_site_name,type))

            for item  in keys:
                keyList[api_site_name] = np.append(keyList[api_site_name],item)


        if save:
            J = zlib.compress(json.dumps(keyList))
            key = bucket.new_key("keyList.json.zlib")
            key.set_contents_from_string(J)

        return keyList


    def makeKeyCount(self,sites,type="questions"):

        keyCount = {}

        for api_site_name in sites.keys():
            #print api_site_name

            keys = bucket.list("%s/%s"%(api_site_name,type))

            for item  in keys:
                if not keyCount.has_key(api_site_name):
                    keyCount[api_site_name] = 1
                else:
                    keyCount[api_site_name] += 1

        return keyCount



    def rand_pick_timeline(self,api_site_name,keyList):
        '''pick a random question timeline'''

        kL = keyList[api_site_name]

        np.random.shuffle(kL)

        for keyS3 in kL[:1]:
            #print "Selected question page : %s" %keyS3

            try:
                questionIds = self.getIds(keyS3)['questionIds']
            except:
                print "[ERROR] could not load question Ids"
                print keyS3

                J = 'error'
                quota = 10000
                already_exists = "error"
                break

            np.random.shuffle(questionIds)

            for q in questionIds[:1]:
                print "Selected question : %s" %q
                J,quota,already_exists = self.timeline(api_site_name,q)

            print "\n"

            if quota == 0:
                break

        return J,quota,already_exists



    def rand_pick_tasks(self,sites,days = 1,threshold = 0.95):
        '''chooses randomly between retrieving a page with questions and or a question timeline. By default function pick the former task in 5% of the cases'''
        from dateutil.relativedelta import relativedelta


        now = datetime.now()
        stop = now + relativedelta(days=days)

        print now,stop

        k=0

        while now < stop :



            now = datetime.now()
            tdiff = stop - now
            print tdiff
            print "hours remaining before  the end : %.2f \n"%((tdiff.days)*24.+tdiff.seconds/3600.)


            '''randomly select a site'''
            api_site_name = self.rand_pick_site(sites)

            print api_site_name

            randNum = np.random.rand()
            #print randNum,threshold

            if randNum > threshold:
                print "questions"
                J,quota = self.questions(api_site_name)

            else:
                print "timeline"
                try:
                    J,quota,already_exists = self.rand_pick_timeline(api_site_name,keyList)
                except:
                    print "no timeline, retrieving some question pages first"
                    J,quota = self.questions(api_site_name)

            time.sleep(3+np.random.rand()*5)


            if quota < 50:
                print "!!! done for today !!!"

                if now > stop:
                    break

                print "probing every 30 minutes until next quota (current time : %s)" % now.isoformat()[:-7]
                time.sleep(3600*0.5)



    def rand_pick_site(self,sites):


        '''Selects a random site according to size distribution'''
        lastPage = []

        site_names = np.array(sites.keys())

        for i,ix in enumerate(site_names):
            lastPage.append(sites[ix]['lastPage'])

        lastPage = np.array(lastPage)
        o = np.argsort(lastPage)

        lastPage = lastPage[o]
        site_names = site_names[o]
        l = np.sum(lastPage)

        rand = np.random.randint(l)

        cumlP = np.cumsum(lastPage)
        index = np.argwhere(cumlP >= rand)[0]
        api_site_name = site_names[index]

        return api_site_name[0]






class ParseJson():
    def __init__(self):
        self.iconn = None
        self.sconn = None

    def parseTimeline(self,site,question):
        print "blah"


    def processQuestions(self,site):

        df_max_size = 300

        l=0
        now = datetime.now()

        questionKeys = bucket.list("%s/questions/"%site)

        allTlines = []


        for j,jx in enumerate(questionKeys):

            if j<601:
                continue

            try:
                jx.get_contents_to_filename('tmp/archive.gz')
                f = gzip.open('tmp/archive.gz', 'rb')
                J = json.loads(f.read())
            except:
                print "problem with %s, skipping" %(jx.name)
                continue

            timelines = []

            for i,question  in enumerate(J['items']):
                qId = question['question_id']
                #print qId
                timelineKeys = bucket.list("%s/timelines/%s"%(site,qId))

                try:
                    for k,kx in enumerate(timelineKeys):
                        kx.get_contents_to_filename('tmp/archive.gz')
                        f = gzip.open('tmp/archive.gz', 'rb')
                        timelineChunk = json.loads(f.read())['items']

                        for item in timelineChunk:
                            if not item['timeline_type'] =='question':
                                timelines.append(self.parseTimeline(item))
                                #timeline['timeline'] = np.concatenate([timeline['timeline'],timelineChunk])
                                #print len(timelines)
                            else:
                                pass

                    timelines.append(self.parseQuestion(question))
                except:
                    print "problem with %s/timelines/%s, skipping" %(site,qId)


            allTlines = np.concatenate([allTlines,timelines])

            if (j%df_max_size) == 0 and j>0:
                print "saving"
                df.save("tmp/DF.pandas")

                key = bucket.new_key("DataFrames/%s/df%s.pandas"%(site,j))
                key.set_contents_from_filename("tmp/DF.pandas")
                os.remove("tmp/DF.pandas")

                allTlines = []
                df = 0


            if j%df_max_size > 0:
                try:
                    df = pandas.concat([df,pandas.DataFrame(timelines)])
                except:
                    df = pandas.DataFrame(timelines)



            dt =  datetime.now() - now
            print j,dt
            now = datetime.now()
            #l+=1
            #if l>4:
            #    return timelines,df
        try:
            print "saving"
            df.save("tmp/DF.pandas")
            key = bucket.new_key("DataFrames/%s/df%s.pandas"%(site,j))
            key.set_contents_from_filename("tmp/DF.pandas")
            os.remove("tmp/DF.pandas")
            return allTlines,df
        except:
            pass

    def parseQuestion(self,inputDic):
        try:
            output = {'timeline_type' : 'question',
                      'question_id' : inputDic['question_id'],
                       'creation_date' : inputDic['creation_date'],
                       'user_id' : inputDic['owner']['user_id'],
                       'attributes' : {'tags' : inputDic['tags'],
                                       'view_count' : inputDic['view_count']
                                       }
                       }

        except KeyError:
            output = {'timeline_type' : 'question',
                      'question_id' : inputDic['question_id'],
                      'creation_date' : inputDic['creation_date'],
                      'user_id' : "00000",
                      'attributes' : {'tags' : inputDic['tags'],
                                       'view_count' : inputDic['view_count']
                                       }
                      }
        return output


    def parseTimeline(self,inputDic):
        try:
            output = {'timeline_type' : inputDic['timeline_type'],
                      'question_id' : inputDic['question_id'],
                      'creation_date' : inputDic['creation_date'],
                      'user_id' : inputDic['owner']['user_id']
                      }
        except KeyError:
            output = {'timeline_type' : inputDic['timeline_type'],
                      'question_id' : inputDic['question_id'],
                      'creation_date' : inputDic['creation_date'],
                      'user_id' : "00000"
                      }


        if inputDic['timeline_type'] == 'vote_aggregate':

            output['attributes'] = {'post_id' : inputDic['post_id'],
                                    'up_vote_count' : inputDic['up_vote_count'],
                                    'down_vote_count' : inputDic['down_vote_count']
                                    }


        elif inputDic['timeline_type'] == "comment":
            output['attributes'] = {'post_id' : inputDic['post_id'],
                                    'comment_id' : inputDic['comment_id']
                                    }

        elif inputDic['timeline_type'] == "accepted_answer":
            output['attributes'] = {'post_id' : inputDic['post_id']}

        return output



if __name__ == '__main__':


    S3 = S3crawl()

    global pageSize
    pageSize = 100

    global bucketName
    bucketName = "stackoverflow_data"

    global bucket
    bucket = S3.connectBucket(bucketName)

    global sites
    #sites = S3crawl.listWebsites()
    key = bucket.get_key("main/sites.json")
    sites = json.loads(key.get_contents_as_string())
    sites.pop("stackoverflow",None)

    global api_site_name
    #api_site_name = "wordpress"


    global keyList
    #keyList = S3.makeKeylistSites({'stackoverflow' : 0},type="timelines")

    '''
    for i,ix in enumerate(sites):
        print i,ix
        ParseJson().processQuestions(ix)
    '''
    '''
    lenQuestions = {}
    for key in keyList.keys():
        lenQuestions[key] = len(keyList[key])
    print np.sum(lenQuestions.values())


    keyCount = S3crawl.makeKeyCount(sites,type="timelines")
    print np.sum(keyCount.values())

    '''

    ''' Retrieve Question Pages'''
    #J = CS.pageQuestions()

    ''' Retrieve Timelines '''


    #J,quota = CS.timeline(4)

    #CS.rand_pick_timeline(n=10)

    #CS.rand_pick_tasks(days = 3)
