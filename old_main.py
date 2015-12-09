

class crawl_stackoverflow():
    def __init__(self):
        self.iconn = None
        self.sconn = None

    def stack_credentials(self):
        credentials = json.loads(open(credentials,"rb").read())

        return credentials
      

                
        
    def retrieveJson(self,url,filename,overwrite=False):
    
        ''' check if file already exists '''
        if os.path.exists(filename) and overwrite==False:
            print "Already downloaded"
        
        
        else:    
            try:
                '''retrieve  gzipped json'''
                req = urllib2.Request(url)
                response = urllib2.urlopen(req)           
                gzipJson= response.read()
                print "successfully retrieved gzipped JSON"
            except:
                print "error retrieving JSON (%s)"%url
            
            try:                
                f = open(filename,'wb')
                f.write(gzipJson)
                f.close()
                print "successfully saved gzipped JSON"
            except:
                print "error saving gzip file"

        try:       
            '''check remaining quota'''
            f = gzip.open(filename, 'rb')
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
        
        return J,has_more,quota
    

    

    
    
    def lastQuestion(self,website):
    
        date = datetime.now().date().strftime("%s")
    
        url ="http://api.stackexchange.com/2.1/questions?key=%s&pagesize=1&fromdate=%s&order=desc&sort=creation&site=%s"%(key,date,website)
        print url
        
        J,has_more,quota = self.retrieveJson(url,"tmp/testlastquestion.gz",overwrite=True)
        try:
            questionId = J['items'][0]['question_id']
        except:
            questionId = 0
            
        return questionId
    
    
    
    def almostLastPage(self,website):
        '''finds an approximation of the last page'''
    
        questionID = self.lastQuestion(website)
    
        #last page approximation 
        
        ratios = np.arange(1.5,5,0.25)
        for r in ratios:
            l = -1  
            pageId = max(1,int(questionID/pageSize/r))            
            print pageId,questionID,pageSize,r
            url = '''https://api.stackexchange.com/2.1/questions?key=%s&page=%s&pagesize=%s&order=asc&sort=creation&site=%s''' %(key,pageId,pageSize,website)
            print url
            #filename = "%s/questions/creation_asc_page%s_psize%s.gz"%(data_dir,pageId,pageSize)
            
            while l==-1:
                J,has_more,quota = self.retrieveJson(url,"tmp/lastPage.gz",overwrite=True)
    
                try:
                    l = len(J['items'])
                except:
                    print J
                    l = -1
                    time.sleep(5)
    
            print l
    
            if l>0:
                print "done"
                break
            
            time.sleep(3)
                
        return J,pageId
    
    
    def pageQuestions(self):
        
        key = self.stack_credentials()['key']
            
        '''check last page downloaded'''


        R = range(1,57140)

        np.random.shuffle(R)

        for pageId in R[:1]:
            url = '''https://api.stackexchange.com/2.1/questions?key=%s&page=%s&pagesize=%s&order=asc&sort=creation&site=stackoverflow''' %(key,pageId,pageSize)
            #url = '''https://api.stackexchange.com/2.1/questions?page=%s&pagesize=%s&order=asc&sort=creation&site=stackoverflow''' %(pageId,pageSize)
            print url
            
            filename = "%s/questions/creation_asc_page%s_psize%s.gz"%(data_dir,pageId,pageSize)

            J,has_more,quota = self.retrieveJson(url,filename)
                
           
            
            if quota == 0:              
                f = open("%s/questions/logCrawl" % data_dir,'a')
                f.write("time : %s, pageId : %s" %datetime.now().isoformat()[:-7],pageId-1)
                f.close()
                break
      
        return J,quota


 


    def getIds(self,filename):        
        f = gzip.open(filename,'rb')
        J = f.read()
        f.close()
        J = json.loads(J)

        questionIds = []

        for i in range(pageSize):
            qId = J['items'][i]['question_id']
            questionIds = np.append(questionIds,qId)
            
        return {'J':J,'questionIds' : map(int,questionIds)}



        '''
        input_file = "%s/questions/creation_asc_page%s_psize%s.gz"%(data_dir,pageId,pageSize)
        
        qIds = self.getIds(input_file)[1]
        print qIds
        idList = ';'.join(map(str,map(int,qIds)))
        print len(idList)
        '''

    def timeline(self,id):

        id = int(id)

        key = self.stack_credentials()['key']

        page = 1

        path = "%s/timelines/%s"%(data_dir,id)
        print path



        if not os.path.exists(path):
            os.mkdir(path)
            
        
        has_more = True

        while has_more == True: 
            url = '''https://api.stackexchange.com/2.1/questions/%s/timeline?site=stackoverflow&page=%s&pagesize=%s&key=%s'''%(id,page,pageSize,key)
            print url
            filename = "%s/page%s_pageSize%s.gz"%(path,page,pageSize)
            print filename
                        
            J,has_more,quota = self.retrieveJson(url,filename)
            print "has_more pages: %s"%has_more
            page +=1
            
         
        return J,quota



    
    def rand_pick_timeline(self,n=1):

        path = 'data/questions'

        qPages = os.listdir(path)

        np.random.shuffle(qPages)

        
        for qp in qPages[:n]:
            
            print "Selected question page : %s" %qp
            filename = "%s/%s"%(path,qp)
            
            try:
                questionIds = self.getIds(filename)['questionIds']
            except:
                print "[ERROR] print could not load question Ids"
                J = 'error'
                quota = 10000
                os.remove(filename)
                print "file removed (%s)" %filename
                
                break
            
            np.random.shuffle(questionIds)
            
            for q in questionIds[:1]:
                print "Selected question : %s" %q
                J,quota = self.timeline(q)
        
            print "\n"

            if quota == 0:
                break

        return J,quota




    def rand_pick_tasks(self,days = 1):
        from dateutil.relativedelta import relativedelta
        
        threshold = 0.95
        
        
        now = datetime.now()
        stop = now + relativedelta(days=days)
        
        print now,stop
        
        while now < stop :

            now = datetime.now()
            tdiff = stop - now
            print tdiff
            print "hours remaining before  the end : %.2f \n"%((tdiff.days)*24.+tdiff.seconds/3600.)
            
            randNum = np.random.rand()
            #print randNum,threshold
            
            if randNum > threshold:
                print "questions"
                J,quota = self.pageQuestions()
                #time.sleep(np.random.rand()*5+15)
            else:
                print "timeline"
                J,quota = self.rand_pick_timeline(n=1)
                #time.sleep(np.random.rand()*5+3)

            time.sleep(4 + np.random.rand()*5)



            
            if quota < 100:
                print "!!! done for today !!!"
                
                if now > stop:
                    break
                
                print "probing every 30 minutes until next quota (current time : %s)" % now.isoformat()[:-7]
                time.sleep(3600*0.5)