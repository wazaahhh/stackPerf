import numpy as np
import pylab as pl
from scipy import stats as S

import boto
import zlib,json
import pandas as pd

def connectBucket(bucketName):
    s3 = boto.connect_s3()
    bucket = s3.get_bucket(bucketName)
    return bucket

global bucketName
bucketName = "stackperf"

global bucket
bucket = connectBucket(bucketName)


def rankorder(x):
	x1 = list(np.sort(x))
	x1.reverse()
	y1 = range(1,len(x1)+1)
	return np.array(x1),np.array(y1)


def logify(x,y):
	from numpy import log10,zeros_like

	x = np.array(x)
	y = np.array(y)

	c = (x>0)*(y>0)

	lx = log10(x[c])
	ly = log10(y[c])

	return lx,ly

def binning(x,y,bins,log_10=False,confinter=5):
    '''makes a simple binning'''

    x = np.array(x);y = np.array(y)

    if isinstance(bins,int) or isinstance(bins,float):
        bins = np.linspace(np.min(x)*0.9,np.max(x)*1.1,bins)
    else:
        bins = np.array(bins)

    if log_10:
        bins = bins[bins>0]
        c = x > 0
        x = x[c]
        y = y[c]
        bins = np.log10(bins)
        x = np.log10(x)
        y = np.log10(y)

    Tbins = []
    Median = []
    Mean = []
    Sigma =[]
    Perc_Up = []
    Perc_Down = []
    Points=[]


    for i,ix in enumerate(bins):
        if i+2>len(bins):
            break

        c1 = x >= ix
        c2 = x < bins[i+1]
        c=c1*c2

        if len(y[c])>0:
            Tbins = np.append(Tbins,np.median(x[c]))
            Median =  np.append(Median,np.median(y[c]))
            Mean = np.append(Mean,np.mean(y[c]))
            Sigma = np.append(Sigma,np.std(y[c]))
            Perc_Down = np.append(Perc_Down,np.percentile(y[c],confinter))
            Perc_Up = np.append(Perc_Up,np.percentile(y[c],100 - confinter))
            Points = np.append(Points,len(y[c]))


    return {'bins' : Tbins,
            'median' : Median,
            'mean' : Mean,
            'stdDev' : Sigma,
            'percDown' :Perc_Down,
            'percUp' :Perc_Up,
            'nPoints' : Points}


def answerCDF(api_site_name):

    try:
        key = bucket.get_key("QA/%s.json.zlib"%api_site_name)
        items = json.loads(zlib.decompress(key.get_contents_as_string()))
    except:
        "print data not found on S3. Please crawl data from stackexchange website first"
        return

    A = []
    S = []
    Sa = []
    rank = []
    score = []
    time = []

    for item in items:
        a = 0
        s = 0
        t0 = item['creation_date']


        if item.has_key('answers'):
            for i,answer in enumerate(item['answers']):

                a += 1
                dt = answer['creation_date'] - t0
                time.append(dt)

                score.append(answer['score'])
                s += answer['score']

                rank.append(i+1)

                #t0 = answer['creation_date']
            A.append(a)
            Sa.append(s)
            S.append(item['score'])

    return {'rank' : np.array(rank),
            'score' : np.array(score),
            'time' : np.array(time),
            'A': np.array(A),
            'S': np.array(S),
            'Sa': np.array(Sa),
            }
