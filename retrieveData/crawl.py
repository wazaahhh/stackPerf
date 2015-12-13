import numpy as np
from datetime import datetime
import urllib2,zlib,json
import time
import boto
import re

#global quota
#quota = 10000

#global has_more
#has_more = False


global pageSize
pageSize = 100

global bucketName
bucketName = "stackperf"

def connectBucket(bucketName):
    s3 = boto.connect_s3()
    bucket = s3.get_bucket(bucketName)
    return bucket

bucket = connectBucket(bucketName)

def loadApiKey():
    #return json.loads(open('credentials','rb').read()[:-1])['key']
    return open('credentials','rb').read()[:-1]

global api_key
api_key = loadApiKey()

def request(api_url):
    req = urllib2.Request(api_url)
    response = urllib2.urlopen(req)
    J = json.loads(zlib.decompress(bytes(bytearray(response.read())),15 + 16))
    quota = J['quota_remaining']
    has_more = J['has_more']
    return J

def retrieveData(api_url,getall=False):
    '''core function to retrieve contents from stackoverflow api'''

    J = request(api_url)
    items = J['items']
    quota = J['quota_remaining']
    has_more = J['has_more']

    if getall:
        page = 1
        while has_more:
            page += 1
            api_url = re.sub("page=.*?\&","page=%s&"%page,api_url)
            print api_url
            try:
                J = request(api_url)
                time.sleep(1)
            except Exception,e:
                print str(e)
                if str(e) == 'BadStatusLine':
                    time.sleep(30)
                    print "sleeping a bit"
                else:
                    break

            items = np.concatenate([items,J['items']])
            quota = J['quota_remaining']
            has_more = J['has_more']
            print has_more, quota

            if quota == 0:
                print "no quota anymore"
                break

    if len(items) ==0:
        #print J
        return 0
    else:
        return list(items)


def lastQuestion(api_site_name):
    date = datetime.now().date().strftime("%s")
    url ="http://api.stackexchange.com/2.1/questions?key=%s&pagesize=1&order=desc&sort=creation&site=%s"%(api_key,api_site_name)
    #print url
    items = retrieveData(url)


    try:
        questionId = items[0]['question_id']
    except:
        questionId = 0

    return questionId


def almostLastPage(api_site_name):
    '''finds an approximation of the last page'''

    #questionId = 0
    #while questionId==0:
    questionId = lastQuestion(api_site_name)
    time.sleep(3)


    ratios = np.arange(1.5,5,0.25)[::-1]
    ratios = np.unique(map(int,questionId/pageSize/ratios))
    for r in ratios:
        l = -1
        pageId = max(1,r)
        print pageId,questionId,pageSize,r
        url = '''https://api.stackexchange.com/2.1/questions?key=%s&page=%s&pagesize=%s&order=asc&sort=creation&site=%s''' %(api_key,pageId,pageSize,api_site_name)
        print url
        #filename = "%s/questions/creation_asc_page%s_psize%s.gz"%(data_dir,pageId,pageSize)

        items = retrieveData(url)
        #print has_more
        time.sleep(5)

        if items == 0:
            break

    return pageId - 1


def listWebsites(reload=False,uploadToS3=False):
    '''provides a list of stackexchange Q&A forums'''

    if not reload:
        key = bucket.get_key("sites/sites.json")
        print "loading site list from S3"
        return json.loads(key.get_contents_as_string())

    url = "http://api.stackexchange.com/2.1/sites?key=%s&pagesize=999&filter=!-rLKUaOi"%api_key
    items = retrieveData(url)

    sites = {}

    for i,ix in enumerate(items):
        api_site_name = ix['api_site_parameter']
        lastQ = lastQuestion(api_site_name)
        sites[api_site_name] = {"lastQuestion": lastQ}
        #print i,api_site_name,lastQ
        time.sleep(2)

    if uploadToS3:
        keyS3 = bucket.new_key("sites/sites.json")
        keyS3.set_contents_from_string(json.dumps(sites))
        print "successfully saved JSON on S3"

    return sites



def retrieveQuestions(api_site_name,reload=False,uploadToS3=True):

    if not reload:
        try:
            key = bucket.get_key("QA/%s.json.zlib"%api_site_name)
            return json.loads(zlib.decompress(key.get_contents_as_string()))
        except:
            pass

    #url = "http://api.stackexchange.com/2.2/questions?key=%s&page=1&pagesize=100&order=asc&sort=creation&site=%s&filter=!1zsgZPC8u*%%29iWh5P43S_k"%(api_key,api_site_name)
    url = "http://api.stackexchange.com/2.2/questions?key=%s&page=1&pagesize=100&order=asc&sort=creation&site=%s&filter=!1zsgZPWgv2QOgMTscL*8F"%(api_key,api_site_name) # added question tags

    print url
    items = retrieveData(url,getall=True)

    if uploadToS3:
        key = bucket.new_key("QA/%s.json.zlib"%api_site_name)
        contents = zlib.compress(json.dumps(items))
        key.set_contents_from_string(contents)
        print "Items successfully saved JSON on S3"

    return items

def retrieveAllSites(exclude=['stackoverflow']):
    siteList = listWebsites()
    for e in exclude:
        siteList.pop(e)

    sites,lq = np.array(zip(*[(k,v['lastQuestion']) for (k,v) in siteList.items()]))
    o = np.argsort(map(int,lq))[::-1]

    for site in sites[o][:10]:
        items = retrieveQuestions(site,reload=False,uploadToS3=True)




def getTimeline(api_site_name,questionId):
        '''retrieve question timeline id'''
        url = '''https://api.stackexchange.com/2.1/questions/%s/timeline?key=%s&page=%s&pagesize=%s&site=%s'''%(questionId,api_key,1,pageSize,api_site_name)
        print url
        items = retrieveData(url,getall=True)
        return items

def rand_pick_timeline(self,api_site_name):
        '''pick a random question timeline'''

        lastQuestion = siteList[api_site_name]["lastQuestion"]
        rangeQuestions = range(1,lastQuestion + 1)

        np.random.shuffle(rangeQuestions)


        # for keyS3 in kL[:1]:
        #     #print "Selected question page : %s" %keyS3
        #
        #     try:
        #         questionIds = self.getIds(keyS3)['questionIds']
        #     except:
        #         print "[ERROR] could not load question Ids"
        #         print keyS3
        #
        #         J = 'error'
        #         quota = 10000
        #         already_exists = "error"
        #         break
        #
        #     np.random.shuffle(questionIds)
        #
        #     for q in questionIds[:1]:
        #         print "Selected question : %s" %q
        #         J,quota,already_exists = self.timeline(api_site_name,q)
        #
        #     print "\n"
        #
        #     if quota == 0:
        #         break
        #
        # return J,quota,already_exists


if __name__ == '__main__':

     global quota
     quota = 10000

     global has_more
     has_more = True

     #global  sites
