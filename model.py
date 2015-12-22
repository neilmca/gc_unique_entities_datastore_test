# library
from google.appengine.ext import ndb

# set up class for looking up url mapping entry from the Datatstore
class VoucherSet(ndb.Model):
	code = ndb.StringProperty(indexed=True)
	created = ndb.IntegerProperty(indexed=True)
	createdDate = ndb.StringProperty(indexed=False)

class LastAssignedVoucher(ndb.Model):
	timeStamp = ndb.IntegerProperty(indexed=True)