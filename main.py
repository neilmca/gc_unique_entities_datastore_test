from google.appengine.api import users
import webapp2
import logging
import base64
from model import VoucherSet
from model import LastAssignedVoucher
# google data store access
from google.appengine.ext import ndb
import re
import datetime
import random
import string
from google.appengine.ext.db import stats
from google.appengine.api import taskqueue
from google.appengine.api import app_identity

VOUCHER_BACKLOG_COUNT = 14
LastAssignedVoucherCursorKey = 'LastAssignedVoucherCursorKey'
BATCH_WRITE_SIZE = 2

class BaseHandler(webapp2.RequestHandler):
    def handle_exception(self, exception, debug):
        # Log the error.
        
        logging.exception(exception)

        # Set a custom message.
        self.response.write('An error occurred.')

        # If the exception is a HTTPException, use its error code.
        # Otherwise use a generic 500 error code.
        if isinstance(exception, webapp2.HTTPException):
            self.response.set_status(exception.code)
        else:
            self.response.set_status(500)


#from looking at app stats http://localhost:8080/_ah/stats/ it can be seen that this query constitutes a single DATASTORE_READ cost
def does_code_exist(code_to_find):
        search_key = ndb.Key(VoucherSet.__name__, code_to_find)        
        found_entity = search_key.get()
        if found_entity == None:
            return False
        else:
            return True

def TimestampMillisec64():
    return int((datetime.datetime.utcnow() - datetime.datetime(1970, 1, 1)).total_seconds() * 1000) 

def UpdateLastAssignedVoucher(lastAssignedtimeStamp):
    ds = LastAssignedVoucher(timeStamp = lastAssignedtimeStamp, id=LastAssignedVoucherCursorKey)
    ds.put()

def GetLastAssignedVoucherTimestamp():
    find_key = ndb.Key(LastAssignedVoucher.__name__, LastAssignedVoucherCursorKey)        
    entity = find_key.get()
    if entity == None:
        #key not existing so all just return 0 - representing the timestamp after a voucher can be assigned
        logging.info('GetLastAssignedVoucherTimestamp - no entry found')
        return 0
    else:
        logging.info('GetLastAssignedVoucherTimestamp - entry found')
        return entity.timeStamp

def GetLastAssignedVoucherEntity():
    find_key = ndb.Key(LastAssignedVoucher.__name__, LastAssignedVoucherCursorKey)        
    lastAssignedEntity = find_key.get()
    if lastAssignedEntity == None:
        #key not existing so all just return 0 - representing the timestamp after a voucher can be assigned
        logging.info('GetLastAssignedVoucherEntity - no entry found')
        return None
    else:
        #get entity from VoucherSet
        resultSet = VoucherSet.query(VoucherSet.created == lastAssignedEntity.timeStamp)
        for result in resultSet:
            return result
        return None

def WriteRecords(entity_write_count):
    
    start = datetime.datetime.now()

    counter = 0
    write_list = []
    full_write_list = []
    while entity_write_count > 0:
        
            while True:
                k = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(6))
              
                if does_code_exist(k) == False:
                    break
                else:
                    logging.info('code conflict ' + k + ' already exists.')
                break;    

            #logging.info('writing new code (#%d) %s' % (counter, k))
            ds = VoucherSet( code = k, created = TimestampMillisec64(), createdDate = str(datetime.datetime.utcnow()), id=k)
            counter=counter+1
            entity_write_count = entity_write_count -1
            
            #single write
            #ds.put()
            
            #multi write
            write_list.append(ds)

            if(len(write_list) >= BATCH_WRITE_SIZE):
                logging.info('writing %s entries to store with put_multi' % len(write_list))
                ndb.put_multi(write_list)    
                full_write_list.extend(write_list)
                write_list = []

    #flush any remaining writes
    if(len(write_list) > 0):
        logging.info('writing %s entries to store with put_multi' % len(write_list))
        full_write_list.extend(write_list)
        ndb.put_multi(write_list) 

    end = datetime.datetime.now()
    logging.info ('written %d entities to datastore in %s mS' % (counter, str((end-start).total_seconds() * 1000)))

    #get count
    GetEntityCount(False)
    CheckForMissingWrites(full_write_list)

def CheckForMissingWrites(write_list):
    
    #get all records in ds
    resultSet = VoucherSet.query().fetch(keys_only=True)
    currentCount = len(resultSet)
    
    logging.info('CheckForMissingWrites resultSet count = %d' % currentCount)
    for entity in resultSet:
        logging.info('ds entity = %s' %  entity.id)           

    for written in write_list:
        logging.info('written record = %s' %  written.code)


def Replenish(): #start replenishing the free vouchers

    currentCount = GetEntityCount()
    
    topupCount = VOUCHER_BACKLOG_COUNT - currentCount
    if(topupCount > 0):
        logging.info('Replenish topping up voucher backlog with %d new vouchers' % topupCount)
        WriteRecords(topupCount)
        return True
    else:
        logging.info('Replenish - no more vouchers to write')
        return False

def GetEntityCount(logout = False):
    kind_stats = stats.KindStat().all().filter("kind_name =", "VoucherSet").get()
        
        
    if(kind_stats == None):
        logging.info('GetEntityCount KindStat returned None')
        #Stats is not working so check to see how many there are by reading all keys
        st = datetime.datetime.now()
        resultSet = VoucherSet.query().fetch(keys_only=True)
        currentCount = len(resultSet)
        en = datetime.datetime.now()
        logging.info('GetEntityCount resultSet count = %d (read duration = %s)' % (currentCount, str((en-st).total_seconds() * 1000)))
        if(logout == True):
            counter = 1
            for entity in resultSet:
                logging.info('#%d entity_key = %s' % (counter, entity))
                counter = counter + 1
    else:
        currentCount = kind_stats.count    


    logging.info('GetEntityCount current number of vouchers in backlog = %d (VOUCHER_BACKLOG_COUNT = %d)' % (currentCount, VOUCHER_BACKLOG_COUNT))



    return currentCount

class AssignVouchersHandler(BaseHandler):

   
    def get(self):

        #path param 1  = total number of vouchers to assign as "used"
        

        #E.g. http://localhost:8080assign=1000

        path = re.sub('^/', '', self.request.path)
        path = re.sub('/$', '', path)

                
        split = path.split('/')

        logging.info(split)
                 
        assignVouchersCount = 0
       
        temp = self.request.get("assign")
        if temp != '':
            assignVouchersCount = int(temp)

        
        processing_start = datetime.datetime.now()        

        responseMsg = '<p>Assigning %d vouchers as used' % assignVouchersCount

        if(assignVouchersCount > 0):

            #get cursor to last assigned voucher
            lastAssignedVoucherCursor = GetLastAssignedVoucherTimestamp()
            remainingVouchersSet = VoucherSet.query(VoucherSet.created > lastAssignedVoucherCursor).fetch(keys_only=True)
            responseMsg += '<p style="text-indent: 2em;">current LastAssignedVoucherCursor = %d, remaining vouchers = %d' % (lastAssignedVoucherCursor, len(remainingVouchersSet))
            
            #fetch all items after the cursor
            resultSet = VoucherSet.query(VoucherSet.created > lastAssignedVoucherCursor).order(VoucherSet.created).fetch(assignVouchersCount)

            responseMsg += '<p style="text-indent: 2em;">vouchers to be assigned'
            assignedCount = 0
            lastEntity = None
            for entity in resultSet:
                assignedCount = assignedCount + 1
                lastEntity = entity
                responseMsg += '<p style="text-indent: 4em;">code = %s, created = %d createdDate = %s' % (entity.code, entity.created, entity.createdDate)

                
            
            responseMsg += '<p style="text-indent: 2em;">Codes requested = %d, Codes assigned = %d' % (assignVouchersCount, assignedCount)
            #update cursor to last assigned code
            if(lastEntity != None):
                UpdateLastAssignedVoucher(lastEntity.created)
            lastAssignedVoucherCursor = GetLastAssignedVoucherTimestamp()
            lastAssignedVoucherCursorEntity = GetLastAssignedVoucherEntity()
            remainingVouchersSet = VoucherSet.query(VoucherSet.created > lastAssignedVoucherCursor).fetch(keys_only=True)
            c = ''
            cr = 0
            cd = 0
            if(lastAssignedVoucherCursorEntity != None):
                c = lastAssignedVoucherCursorEntity.code
                cr = lastAssignedVoucherCursorEntity.created
                cd = lastAssignedVoucherCursorEntity.createdDate
            responseMsg += '<p style="text-indent: 2em;">LastAssignedVoucherCursor moved to %d (code=%s, created=%d, createdDate=%s), remaining vouchers = %d' % (lastAssignedVoucherCursor, c, cr, cd,len(remainingVouchersSet))
        
            #verbose listing of all vouchers highlighting the current position of the cursor
            resultSet = VoucherSet.query().order(VoucherSet.created)

            responseMsg += '<p style="text-indent: 2em;">All vouchers'
            for entity in resultSet:
                if(lastAssignedVoucherCursorEntity.code == entity.code):
                    #highlight
                    responseMsg += '<p style="text-indent: 2em; color:red;">code = %s, created = %d createdDate = %s' % (entity.code, entity.created, entity.createdDate)
                else:
                    responseMsg += '<p style="text-indent: 2em;">code = %s, created = %d createdDate = %s' % (entity.code, entity.created, entity.createdDate)

        end = datetime.datetime.now()
        responseMsg += '<p style="text-indent: 2em; ">total processing time (mS) = %s' % str((end-processing_start).total_seconds() * 1000)
                               

        self.response.write(responseMsg)


class CronCountCodesHandler(BaseHandler):
   
   
    def get(self):
        GetEntityCount(True)



class CronReplenishEnqHandler(BaseHandler):

    #from experimentation  
        # time taken to write 1000 entries = 84s
        # time tkane to write 10,000 enties > 10mins => exceeds the 10min cut off for a task associated to an automatically scaled module.
   
    def get(self):

        #invoke task queue
        # Add the task to the default queue.
        taskqueue.add(queue_name='replenish-queue', url='/taskqueue_handler', params={'trigger': 'cron'})


    
class TaskQueueHandler(BaseHandler):

  
   
     def post(self): 
        trigger = self.request.get('trigger')
        logging.info('TaskQueueHandler called from %s' % trigger)
        if(Replenish() == True):
            return
            #more work to do, so kick off another task
            #taskqueue.add(queue_name='replenish-queue', url='/taskqueue_handler', params={'trigger': 'queue'})
        
        #no more work to do




logging.getLogger().setLevel(logging.DEBUG)





logging.info('Serving Url = %s' % app_identity.get_default_version_hostname())

app = webapp2.WSGIApplication([
    ('/cron_replenish', CronReplenishEnqHandler),
    ('/cron_count_codes', CronCountCodesHandler),
    ('/taskqueue_handler', TaskQueueHandler),
    ('/.*', AssignVouchersHandler)    
    
], debug=True)



