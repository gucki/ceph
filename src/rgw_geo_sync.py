#!/usr/bin/env python

import boto
import datetime
import json
import logging
import random
import requests
import string
import sys
import time

import boto.s3.acl
import boto.s3.connection

from boto.connection import AWSAuthConnection
from operator import attrgetter

logging.basicConfig(filename='boto.log', level=logging.DEBUG)
log = logging.getLogger(__name__)

access_key_source = '85OKY95S2US1LB4Z4ZYI' #buck
secret_key_source = 'wy4DkpcGG+fAuOMfYPYAI37P7Qe2kg+oQpZIqByk'
access_key_dest = '95OKY95S2US1LB4Z4ZYI'
secret_key_dest = 'zy4DkpcGG+fAuOMfYPYAI37P7Qe2kg+oQpZIqByl'
log_lock_time = 60 #seconds
#debug_commands = True
debug_commands = False
#deep_compare = True
deep_compare = False

# generates an N character random string from letters and digits
local_lock_id = ''.join(random.choice(string.ascii_letters + string.digits) for x in range(16)) #32 digits for now

def add_user_to_remote(source_conn, dest_conn, uid):

    # working around botched account on my install
    if uid == 'inktank:greg':
      return

    # get current info for this uid. 
    (ret, src_acct) = rgwadmin_rest(source_conn, ['metadata', 'userget'], {'key':uid})
    if 200 != ret:
        print 'in add_user_to_remote source side metadata user (GET) failed, return http code: ', ret
        return ret

    args = {}
    args['key'] = src_acct()['data']['user_id']

    outData = json.dumps(src_acct())

    (ret, dest_acct) = rgwadmin_rest(dest_conn, ['metadata', 'userput'], args, data=outData)
    if 200 != ret:
        print 'metadata user (PUT) failed, return http code: ', ret
        print 'body: ', dest_acct()
        return ret


    return ret

# for now, just reuse the add_user code. May need to differentiate in the future
def update_remote_user(source_conn, dest_conn, uid):
    return add_user_to_remote(source_conn, dest_conn)

def manually_diff(source_data, dest_data):
    diffkeys1 = [k for k in source_data if source_data[k] != dest_data[k]]
    diffkeys2 = [k for k in dest_data if dest_data[k] != source_data[k]]

    #retKeys = dict(diffkeys1.items() + diffkeys2.items())
    return diffkeys1 + diffkeys2

# for a given user, 
def check_individual_user(source_conn, dest_conn, uid, tag=None, manual_validation=False):
    retVal = 200

    if uid == "inktank:greg":
      return

    (ret, out) = rgwadmin_rest(source_conn, ['metadata', 'userget'], {"key":uid})
    if 200 != ret:
        print 'source side metadata user (GET) failed, returned http code', ret
        return ret

    source_data = out()

    # if the tag on the source_data does not match, skip this user
    if tag != None and tag != source_data['ver']['tag']:
        print 'log tag ', tag, ' != current uid tag ', source_data['ver']['tag'], '. Skipping this uid / tag pair (', uid, " / ", tag, ")"
        return

    (ret, out) = rgwadmin_rest(dest_conn, ['metadata', 'userget'], {"key":uid})
    if 200 != ret and 404 != ret:
        print 'destination metadata user (GET) failed, returned http code', ret
        return ret

    dest_data = out()

    if manual_validation:
        print 'manually validating ', uid
        diff_set = manually_diff(source_data, dest_data)

        if 0 == len(diff_set):
            print 'deep comparison of uid ', uid, ' passed'
        else:
            for k in diff_set:
                print k, ':', source_data[k], ' != ', dest_data[k]

        return

    # if this user does not exist on the destination side, add it
    if (ret == 404 and dest_data['Code'] == 'NoSuchKey'):
        print 'user: ', uid, ' missing from the remote side. Adding it'
        retVal = add_user_to_remote(source_conn, dest_conn, uid)

    else: # if the user exists on the remote side, ensure they're the same version
        dest_ver = dest_data['ver']['ver']
        source_ver = source_data['ver']['ver']

        if dest_ver != source_ver:
            print 'uid: ', uid, ' local_ver: ', source_ver, ' != dest_ver: ', dest_ver, ' UPDATING'
            retVal = update_remote_user(source_conn, dest_conn, uid)
        else:
            print 'uid: ', uid, ' local_ver: ', source_ver, ' == dest_ver: ', dest_ver 

    return retVal

# metadata changes are grouped into shards based on [ uid | tag ] TODO, figure this out
def process_source_shard(source_conn, dest_conn, shard_num, manual_validation=False):

    if debug_commands:
        print 'lock id:', local_lock_id 

    really_old_time = "2010-10-10 12:12:00"
    sync_start_time = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # first, lock the log
    (ret, out) = rgwadmin_rest(source_conn, ['log', 'lock'], {"type":"metadata", "id":shard_num, "length":log_lock_time, "lock_id":local_lock_id})
    if debug_commands:
        print 'lock log returned: ', ret

    if 200 != ret:
        print 'lock log failed, returned http code: ', ret

    (ret, out) = rgwadmin_rest(source_conn, ['log', 'list', 'id=' + str(shard_num)], 
      {"type":"metadata", "id":shard_num})
    if debug_commands:
        print 'metadata list returned: ', ret

    if 200 != ret:
        print 'metadata list failed, returned http code: ', ret

    log_entry_list = out()

    perUserVersionDict = {}

    print 'shard ', shard_num, ' has ', len(log_entry_list), ' entries'
    # sort the data by reverse status (so write comes prior to completed)
    mySorted = sorted(log_entry_list, key=lambda entry: entry['data']['status']['status'], reverse=True)
    # then sort by read_version
    mySorted2 = sorted(mySorted, key=lambda entry: int(entry['data']['read_version']['ver']))        
    # finally, by name. Not that Python's sort is stable, so the end result is the entries sorted by
    # name, then by version and finally by status.
    mySorted3 = sorted(mySorted2, key=lambda entry: entry['name'])        

    for entry in mySorted3:
        # use both the uid and tag as the key since a uid may have been deleted and re-created
        name = entry['name']
        tag = entry['data']['write_version']['tag']
        ver = entry['data']['write_version']['ver']
        compKey = name + "@" + tag
        # test if there is already an entry in the dictionary for the user
        if (perUserVersionDict.has_key(compKey)): 
            if (perUserVersionDict[compKey] < ver):
                perUserVersionDict[compKey] = ver
        else:  
            perUserVersionDict[compKey] = ver

    for key in perUserVersionDict.keys():
        uid, tag = key.split('@')
        retVal = check_individual_user(source_conn, dest_conn, uid, tag=tag, manual_validation=manual_validation)

    # trim the log
    (ret, out) = rgwadmin_rest(source_conn, ['log', 'trim', 'id=' + str(shard_num)], {"id":shard_num, "type":"metadata", "start-time":really_old_time, "end-time":sync_start_time})

    if 200 != ret:
        print 'log trim returned http code ', ret

    # finally, unlock the log
    (ret, out) = rgwadmin_rest(source_conn, ['log', 'unlock'], {"type":"metadata", "id":shard_num, "lock_id":local_lock_id})

    if debug_commands:
        print 'metadata log unlock returned: ', ret

    if 200 != ret:
        print 'metadata log unlock failed, returned http code: ', ret

def rgwadmin_rest(connection, cmd, params=None, headers=None, raw=False, data=None):
    log.info('radosgw-admin-rest: %s %s' % (cmd, params))
    put_cmds = ['create', 'link', 'add', 'userput']
    post_cmds = ['unlink', 'modify', 'lock', 'unlock']
    delete_cmds = ['trim', 'rm', 'process']
    get_cmds = ['check', 'info', 'show', 'list', 'get', 'userget']

    bucket_sub_resources = ['object', 'policy', 'index']
    user_sub_resources = ['subuser', 'key', 'caps']
    #zone_sub_resources = ['pool', 'log', 'garbage']
    zone_sub_resources = ['pool', 'garbage']
    mdlog_sub_resources= ['mdlog']
    log_sub_resources = ['lock', 'unlock', 'trim']

    def get_cmd_method_and_handler(cmd):
        if cmd[1] in put_cmds:
            print 'in PUT'
            return 'PUT', requests.put
        elif cmd[1] in delete_cmds:
            return 'DELETE', requests.delete
        elif cmd[1] in post_cmds:
            return 'POST', requests.post
        elif cmd[1] in get_cmds:
            return 'GET', requests.get

    def get_resource(cmd):
        if cmd[0] == 'bucket' or cmd[0] in bucket_sub_resources:
            if cmd[0] == 'bucket':
                return 'bucket', ''
            else:
                return 'bucket', cmd[0]
        elif cmd[0] == 'user' or cmd[0] in user_sub_resources:
            if cmd[0] == 'user':
                return 'user', ''
            else:
                return 'user', cmd[0]
        elif cmd[0] == 'usage':
            return 'usage', ''
        elif cmd[0] == 'zone' or cmd[0] in zone_sub_resources:
            if cmd[0] == 'zone':
                return 'zone', ''
            else:
                return 'zone', cmd[0]
        elif cmd[0] == 'metadata':
            if cmd[1] == 'userget' or cmd[1] == 'userput':
                return 'metadata/user', ''
            else:
                return 'metadata', ''
        elif cmd[0] == 'log':
            #if len(cmd) == 2 and cmd[1]=='list':
            #    return 'log/'+cmd[1], ''
            if len(cmd) == 3 and cmd[1]=='list':
                return 'log', cmd[2]
            elif len(cmd) == 2 and cmd[1]=='lock':
                return 'log', cmd[1]
            elif len(cmd) == 2 and cmd[1]=='unlock':
                return 'log', cmd[1]
            elif len(cmd) == 3 and cmd[1]=='trim':
                return 'log', cmd[2]
            else:
                #return 'log', cmd[1]
                return 'log', ''

    """
        Adapted from the build_request() method of boto.connection
    """
    def build_admin_request(conn, method, resource = '', headers=None, data='',
            query_args=None, params=None):

        path = conn.calling_format.build_path_base('admin', resource)
        auth_path = conn.calling_format.build_auth_path('admin', resource)
        host = conn.calling_format.build_host(conn.server_name(), 'admin')
        if query_args:
            path += '?' + query_args
            boto.log.debug('path=%s' % path)
            auth_path += '?' + query_args
            boto.log.debug('auth_path=%s' % auth_path)

        if debug_commands:
            print 'debug print. path: ', path, ' params ', params, ' headers ', headers, ' data', data, ' host', host

        retRequest = AWSAuthConnection.build_base_http_request(conn, method, path,
                auth_path, params, headers, data, host)

        return retRequest

    if headers is None:
      headers = {}

    headers['Content-Type'] = 'application/json; charset=UTF-8'
    #headers['Content-Type'] = 'application/x-www-form-urlencoded'
    #headers['HTTP_TRANSFER_ENCODING'] = 'chunked'

    method, handler = get_cmd_method_and_handler(cmd)
    resource, query_args = get_resource(cmd)
    if data:
      request = build_admin_request(connection, method, resource,
                query_args=query_args, headers=headers, data=data, params=params)
    else:
      request = build_admin_request(connection, method, resource,
                query_args=query_args, headers=headers)

    url = '{protocol}://{host}{path}'.format(protocol=request.protocol,
            host=request.host, path=request.path, params=params)

    tmpHeaders = request.headers
    request.authorize(connection=connection)

    if data:
      result = handler(url, params=params, headers=request.headers, data=data)
    else:
      result = handler(url, params=params, headers=request.headers)

    if raw:
        return result.status_code, result.txt
    else:
        return result.status_code, result.json

def main(user_to_sync=None):

  conn_source = boto.s3.connection.S3Connection(
    aws_access_key_id = access_key_source,
    aws_secret_access_key = secret_key_source,
    is_secure=False,
    #port=7280,
    host = 'rgw-n1',
    calling_format = boto.s3.connection.OrdinaryCallingFormat(),
    debug=2
  )

  conn_dest = boto.s3.connection.S3Connection(
    aws_access_key_id = access_key_dest,
    aws_secret_access_key = secret_key_dest,
    is_secure=False,
    #port=7280,
    host = 'rgw-n2',
    calling_format = boto.s3.connection.OrdinaryCallingFormat(),
  )

  if None != user_to_sync:
      print 'syncing an individual user'
      retVal = check_individual_user(conn_source, conn_dest, user_to_sync)
      return

  (ret, out) = rgwadmin_rest(conn_source, ['metadata', 'list', 'user'],)

  if debug_commands:
      print 'metadata list user ret: ', ret 
      print 'out: ', out()

  if 200 != ret:
      print 'metadata list user failed, ret http code: ', ret

  (ret, out) = rgwadmin_rest(conn_source, ['log', 'list', 'type=metadata'], {"type":"metadata"})
  if debug_commands:
      print 'log list type:metadata ret: ', ret
      print 'out: ', out()

  if 200 != ret:
      print 'log list type:metadata failed, ret http code: ', ret

  numObjects = out()['num_objects']
  print 'We have ', numObjects, ' master objects to check'

  for i in xrange(numObjects):
    #process_source_shard(conn_source, conn_dest, out())
    process_source_shard(conn_source, conn_dest, i)

  if deep_compare:
      for i in xrange(numObjects):
          process_source_shard(conn_source, conn_dest, i, manual_validation=deep_compare)

if __name__ == '__main__':
  if len(sys.argv) == 2:
      print 'synching only user: ', sys.argv[1]
      main(sys.argv[1])
  else:
      main()

