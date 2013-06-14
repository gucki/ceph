
import boto
import datetime
import json
import logging
import random
#import requests
import string
import sys
import time

import boto.s3.acl
import boto.s3.connection

from boto.connection import AWSAuthConnection
from operator import attrgetter
from RGW_REST_factory import RGW_REST_factory

log_lock_time = 60 #seconds

#debug_commands = True
debug_commands = False
#deep_compare = True
deep_compare = False

class REST_metadata_syncher:
    logging.basicConfig(filename='boto_metadata_syncher.log', level=logging.DEBUG)
    log = logging.getLogger(__name__)


    source_conn = None
    dest_conn = None
    local_lock_id = None
    rest_factory = None

    def __init__(self, source_access_key, source_secret_key, source_host,
                       dest_access_key, dest_secret_key, dest_host):

        # generates an N character random string from letters and digits 16 digits for now
        self.local_lock_id = ''.join(random.choice(string.ascii_letters + string.digits) for x in range(16))
    
        # construct the two connection objects
        self.source_conn = boto.s3.connection.S3Connection(
            aws_access_key_id = source_access_key,
            aws_secret_access_key = source_secret_key,
            is_secure=False,
            host = source_host,
            calling_format = boto.s3.connection.OrdinaryCallingFormat(),
            debug=2
        )

        self.dest_conn = boto.s3.connection.S3Connection(
          aws_access_key_id = dest_access_key,
          aws_secret_access_key = dest_secret_key,
          is_secure=False,
          host = dest_host,
          calling_format = boto.s3.connection.OrdinaryCallingFormat(),
        )

        self.rest_factory = RGW_REST_factory()

        if None == self.rest_factory:
            print "RGW_REST_factory() is None. Whooops"


    # we explicitly specify the connection to use for the locking here  
    # in case we need to lock a non-master log file
    def acquire_log_lock(self, conn, lock_id, shard_num):
        (ret, out) = self.rest_factory.rest_call(conn, ['log', 'lock'], 
            {"type":"metadata", "id":shard_num, "length":log_lock_time, "lock_id":lock_id})

        if 200 != ret:
            print 'acquire_log_lock failed, returned http code: ', ret
        elif debug_commands:
            print 'acquire_log_lock returned: ', ret

        return ret


    def add_user_to_remote(self, uid):

        # working around botched account on my install
        if uid == 'inktank:greg':
            return

        # get current info for this uid. 
        (ret, src_acct) = self.rest_factory.rest_call(source_conn, ['metadata', 'userget'], {'key':uid})
        if 200 != ret:
            print 'add_user_to_remote source side metadata user (GET) failed, code: ', ret
            return ret
        elif debug_commands:
            print 'add_user_to_remote metadata userget() returned: ', ret

        # create an empty dict and pull out the user_id to use as an argument for next call
        args = {}
        args['key'] = src_acct()['data']['user_id']

        # json encode the data
        outData = json.dumps(src_acct())

        (ret, dest_acct) = self.rest_factory.rest_call(dest_conn, ['metadata', 'userput'], args, data=outData)
        if 200 != ret:
            print 'metadata user (PUT) failed, return http code: ', ret
            print 'body: ', dest_acct()
            return ret
        elif debug_commands:
            print 'add_user_to_remote metadata userput() returned: ', ret


        return ret

     # for now, just reuse the add_user code. May need to differentiate in the future
    def update_remote_user(self, uid):
        return self.add_user_to_remote(uid)

    # only used for debugging at present
    def manually_diff(self, source_data, dest_data):
        diffkeys1 = [k for k in source_data if source_data[k] != dest_data[k]]
        diffkeys2 = [k for k in dest_data if dest_data[k] != source_data[k] and not (k in source_data) ]

        return diffkeys1 + diffkeys2

    # use the specified connection as it may be pulling metadata from any rgw
    def pull_metadata_for_uid(self, conn, uid):
        (retVal, out) = self.rest_factory.rest_call(conn, ['metadata', 'userget'], {"key":uid})
        if 200 != retVal:
            print 'metadata user(GET) failed for {uid} returned {val}'.format(uid=udi,val=ret)
            return retVal, None
        else:
            if debug_commands:
                print 'pull_metadata_for_uid for {uid} returned: {val}'.format(uid=uid,val=ret)

            return retVal, out()

    def manually_diff_individual_user(self, source_conn, dest_conn, uid):
        print 'manually validating ', uid

        retVal = 200 # default to success

        diff_set = self.manually_diff(source_data, dest_data)

        if 0 == len(diff_set):
            print 'deep comparison of uid ', uid, ' passed'
        else:
            for k in diff_set:
                print k, ':', source_data[k], ' != ', dest_data[k]
                retVal = 400 # throw an http-ish error code

        return retVal

    # for a given user, 
    def check_individual_user(self, uid, tag=None):
        retVal = 200

        # again, this is to work around a corrupted account in my install
        if uid == "inktank:greg":
            return retVal

        retVal, source_data = self.pull_metadata_for_uid(source_conn, uid)
        if 200 != retVal:
            return retVal

        # If the tag on the source_data does not match, skip this user.
        # We assume this is caused by this entry being for a uid that 
        # has been deleted.
        if tag != None and tag != source_data['ver']['tag']:
            print 'log tag ', tag, ' != current uid tag ', source_data['ver']['tag'], \
                  '. Skipping this uid / tag pair (', uid, " / ", tag, ")"
            return retVal

        retVal, dest_data = self.pull_metadata_for_uid(dest_conn, uid)
        if 200 != retVal:
            return retVal

        # if this user does not exist on the destination side, add it
        if (retVal == 404 and dest_data['Code'] == 'NoSuchKey'):
            print 'user: ', uid, ' missing from the remote side. Adding it'
            retVal = self.add_user_to_remote(source_conn, dest_conn, uid)

        else: # if the user exists on the remote side, ensure they're the same version
            dest_ver = dest_data['ver']['ver']
            source_ver = source_data['ver']['ver']

            if dest_ver != source_ver:
                print 'uid: ', uid, ' local_ver: ', source_ver, ' != dest_ver: ', dest_ver, ' UPDATING'
                retVal = self.update_remote_user(source_conn, dest_conn, uid)

            elif debug_commands:
                print 'uid: ', uid, ' local_ver: ', source_ver, ' == dest_ver: ', dest_ver 

        return retVal

    # metadata changes are grouped into shards based on [ uid | tag ] TODO, figure this out
    def process_shard(self, shard_num):

        # we need this due to a bug in rgw that isn't auto-filling in sensible defaults
        # when start-time is omitted
        really_old_time = "2010-10-10 12:12:00"

        # NOTE rgw deals in UTC time. Make sure you adjust your calls accordingly
        sync_start_time = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        # first, lock the log
        self.acquire_log_lock(self.source_conn, self.local_lock_id, shard_num)

        (ret, out) = self.rest_factory.rest_call(self.source_conn, ['log', 'list', 'id=' + str(shard_num)], 
                                   {"type":"metadata", "id":shard_num})

        if 200 != ret:
            print 'metadata list failed, returned http code: ', ret
        elif debug_commands:
            print 'metadata list returned: ', ret

        log_entry_list = out()
        perUserVersionDict = {}

        print 'shard ', shard_num, ' has ', len(log_entry_list), ' entries'
        # sort the data by reverse status (so write comes prior to completed)
        mySorted = sorted(log_entry_list, key=lambda entry: entry['data']['status']['status'], 
                          reverse=True)
        # then sort by read_version
        mySorted2 = sorted(mySorted, key=lambda entry: int(entry['data']['read_version']['ver']))      
        # finally, by name. Not that Python's sort is stable, so the end result is the 
        # entries sorted by name, then by version and finally by status.
        mySorted3 = sorted(mySorted2, key=lambda entry: entry['name'])        

        # iterate over the sorted keys to find the highest logged version for each
        # uid / tag combo (unique instance of a given uid
        for entry in mySorted3:
            # use both the uid and tag as the key since a uid may have been deleted and re-created
            name = entry['name']
            tag = entry['data']['write_version']['tag']
            ver = entry['data']['write_version']['ver']
            compKey = name + "@" + tag

            # test if there is already an entry in the dictionary for the user
            if (perUserVersionDict.has_key(compKey)): 
                # if there is, then only add this one if the ver is higher
                if (perUserVersionDict[compKey] < ver):
                    perUserVersionDict[compKey] = ver
            else: # if not, just add this entry
                perUserVersionDict[compKey] = ver

        # sync each uid / tag pair
        for key in perUserVersionDict.keys():
            uid, tag = key.split('@')
            retVal = self.check_individual_user(uid, tag=tag)

        # trim the log for this shard now that all the users are synched
        (ret, out) = self.rest_factory.rest_call(self.source_conn, ['log', 'trim', 'id=' + str(shard_num)], 
                      {"id":shard_num, "type":"metadata", "start-time":really_old_time, 
                       "end-time":sync_start_time})

        if 200 != ret:
            print 'log trim returned http code ', ret
        elif debug_commands:
            print 'log trim returned http code ', ret

        # finally, unlock the log
        (ret, out) = self.rest_factory.rest_call(self.source_conn, ['log', 'unlock'], {
                                   "type":"metadata", "id":shard_num, "lock_id":self.local_lock_id})

        if 200 != ret:
            print 'metadata log unlock failed, returned http code: ', ret
        elif debug_commands:
            print 'metadata log unlock returned: ', ret

    def sync_all_shards(self):
        (ret, out) = self.rest_factory.rest_call(self.source_conn, ['log', 'list', 'type=metadata'], {"type":"metadata"})

        if 200 != ret:
            print 'log list type:metadata failed, code: ', ret
        elif debug_commands:
            print 'log list type:metadata returned code: ', ret
            print 'out: ', out()

        numObjects = out()['num_objects']
        print 'We have ', numObjects, ' master objects to check'

        for i in xrange(numObjects):
            self.process_shard(i)

    def sync_one_user(self, uid):
        retVal = self.check_individual_user(uid)

