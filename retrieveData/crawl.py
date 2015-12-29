import numpy as np
from datetime import datetime
import urllib2,zlib,json
import time
import boto
import re

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

def retrieveData(api_url, minpage = 1, maxpage = 1):
    '''core function to retrieve contents from stackoverflow api'''

    if maxpage == -1:
        maxpage = 100000

    has_more = True
    page = minpage

    items = []

    while has_more and page <= maxpage:

        api_url = re.sub("page=.*?\&","page=%s&"%page,api_url)
        print api_url
        try:
            J = request(api_url)
            time.sleep(5)
        except:
            print "sleeping a bit"
            time.sleep(5)
            page -= 1

        items = np.concatenate([items,J['items']])
        quota = J['quota_remaining']
        has_more = J['has_more']
        print has_more, quota

        if quota == 0:
            print "no quota anymore"
            break

        page += 1

    if len(items) ==0:
        return 0
    else:
        return {'items' : list(items),'quota' : quota}


def lastQuestion(api_site_name):
    date = datetime.now().date().strftime("%s")
    url ="http://api.stackexchange.com/2.1/questions?key=%s&pagesize=1&order=desc&sort=creation&site=%s"%(api_key,api_site_name)
    #print url
    items = retrieveData(url)['items']


    try:
        questionId = items[0]['question_id']
    except:
        questionId = 0

    return questionId


def listWebsites(reload=False,uploadToS3=False):
    '''provides a list of stackexchange Q&A forums'''

    if not reload:
        key = bucket.get_key("sites/sites.json")
        print "loading site list from S3"
        return json.loads(key.get_contents_as_string())

    url = "http://api.stackexchange.com/2.1/sites?key=%s&pagesize=999&filter=!-rLKUaOi"%api_key
    items = retrieveData(url)['items']

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
    '''retrieve all questions for a given website'''

    if api_site_name == "stackoverflow":
        print "please use retrieveStackoverflow function instead"
        return

    if not reload:
        try:
            key = bucket.get_key("QA/%s.json.zlib"%api_site_name)
            data = json.loads(zlib.decompress(key.get_contents_as_string()))
            print "%s: items loaded from S3"%api_site_name
            return data
        except:
            pass

    #url = "http://api.stackexchange.com/2.2/questions?key=%s&page=1&pagesize=100&order=asc&sort=creation&site=%s&filter=!1zsgZPC8u*%%29iWh5P43S_k"%(api_key,api_site_name)
    url = "http://api.stackexchange.com/2.2/questions?key=%s&page=1&pagesize=100&order=asc&sort=creation&site=%s&filter=!1zsgZPWgv2QOgMTscL*8F"%(api_key,api_site_name) # added question tags

    print url
    items = retrieveData(url,maxpage= -1)['items']

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

def retrieveStackoverflow(uploadToS3=True):
    '''special function to collect stackoverflow data'''

    api_site_name = "stackoverflow"

    keyList = bucket.list("QA/stackoverflow/")

    maxpage = 0



    for k,kx in enumerate(keyList):
        print k,kx.name # search for the largest page already crawled
        if kx.name == "QA/stackoverflow/":
            continue

        try:
            min_p,max_p = map(int,re.findall("stackoverflow/(.*?)_(.*?).json.zlib",kx.name)[0])
            if max_p > maxpage:
                maxpage = max_p
        except:
            print "folder empty"
            break


    url = '''http://api.stackexchange.com/2.2/questions?page=1&pagesize=100&order=asc&sort=creation&key=%s&site=%s&filter=!1zsgZPWgv2QOgMTscL*8F'''%(api_key,api_site_name) # added question tags

    #print url
    quota = 10000
    while quota > 1000:
        minpage = maxpage + 1
        maxpage = minpage + 999

        print minpage,maxpage

        dic_output = retrieveData(url, minpage = minpage, maxpage = maxpage)
        items = dic_output['items']
        quota = dic_output['quota']

        if uploadToS3:
            key = bucket.new_key("QA/stackoverflow/%s_%s.json.zlib"%(minpage,maxpage))
            contents = zlib.compress(json.dumps(items))
            key.set_contents_from_string(contents)
            print "Items successfully saved JSON on S3"




if __name__ == '__main__':

     global quota
     quota = 10000

     global has_more
     has_more = True
