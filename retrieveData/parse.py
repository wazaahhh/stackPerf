import numpy as np
from datetime import datetime
import urllib2,zlib,json
import boto



def connectBucket(bucketName):
    s3 = boto.connect_s3()
    bucket = s3.get_bucket(bucketName)
    return bucket

def retrieveTimeline(api_site_name,questionId):

    prefix = "%s/timelines/%s/"%(api_site_name,questionId)
    #print prefix
    list = bucket.list(prefix)
    items = []

    for k in list:
        print k.name
        J = json.loads(zlib.decompress(bytes(bytearray(k.get_contents_as_string())),15 + 16))
        item = J['items']
        #print len(item)
        items = np.concatenate([items,item])

    return items


def parseItem(itemDic):
    try:
        output = {'timeline_type' : itemDic['timeline_type'],
                  'question_id' : itemDic['question_id'],
                  'creation_date' : itemDic['creation_date'],
                  'user_id' : itemDic['owner']['user_id']
                  }
    except KeyError:
        #print "blah", itemDic
        return
        # output = {'timeline_type' : itemDic['timeline_type'],
        #           'question_id' : itemDic['question_id'],
        #           'creation_date' : itemDic['creation_date'],
        #           'user_id' : "00000"
        #           }


    if itemDic['timeline_type'] == 'vote_aggregate':

        output['attributes'] = {'post_id' : itemDic['post_id'],
                                'up_vote_count' : itemDic['up_vote_count'],
                                'down_vote_count' : itemDic['down_vote_count']
                                }


    elif itemDic['timeline_type'] == "comment":
        output['attributes'] = {'post_id' : itemDic['post_id'],
                                'comment_id' : itemDic['comment_id']
                                }

    elif itemDic['timeline_type'] == "accepted_answer":
        output['attributes'] = {'post_id' : itemDic['post_id']}

    return output


def parseTimeline(api_site_name,questionId):

    items =  retrieveTimeline(api_site_name,questionId)

    parsedItems = np.array([])
    for i,ix in enumerate(items):
        #try:
        parsedItem = parseItem(ix)
        if parsedItem == None:
            print None
            continue
        else:
            print [parsedItem['creation_date'],parsedItem]
            parsedItems = np.concatenate([parsedItems,[parsedItem['creation_date'],parsedItem]])
        #except:
        #    continue

    parsedItems = parsedItems.reshape([len(parsedItems)/2,2])
    o = np.argsort(parsedItems[:,0])
    return parsedItems[o]



if __name__ == '__main__':
    global bucketName
    bucketName = "stackoverflow_data"

    bucket = connectBucket(bucketName)
