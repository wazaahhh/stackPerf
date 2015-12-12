import boto

import json

import pandas as pd

def connectBucket(bucketName):
    s3 = boto.connect_s3()
    bucket = s3.get_bucket(bucketName)
    return bucket

def getSites():
    key = bucket.get_key("main/sites.json")
    sites = json.loads(key.get_contents_as_string())
    sites.pop("stackoverflow",None)
    return sites

def retrieveDF(site):

    key = bucket.get_key("DataFrames/%s"%site)

#def parseQuestion



if __name__ == '__main__':

    global bucketName
    bucketName = "stackoverflow_data"
    
    global bucket
    bucket = connectBucket(bucketName)

    global sites
    sites = getSites()

    
    
#J = json.loads(zlib.decompress(bytes(bytearray(open("tmp/archive.gz",'rb').read())),15 + 16))
#J = json.loads(zlib.decompress(bytes(bytearray(key.get_contents_as_string())),15 + 16))
        
    