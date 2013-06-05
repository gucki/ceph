#!/usr/bin/env python

import logging
import json
import sys
import requests
import boto
import time
import binascii
import inspect

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

def add_user_to_remote(source_conn, dest_conn, uid, tag, ver):

    if uid == 'inktank:greg':
      return

    # get current info for this uid. 
    (ret, src_acct) = rgwadmin_rest(source_conn, ['metadata', 'userget'], {'key':uid})

    args = {}
    args['key'] = src_acct()['data']['user_id']

    outData = json.dumps(src_acct)

    (ret, dest_acct) = rgwadmin_rest(dest_conn, ['metadata', 'userput'], args, data=outData)

# for now, just reuse the add_user code. May need to differentiate in the future
def update_remote_user(source_conn, dest_conn, uid, tag, ver):
    add_user_to_remote(source_conn, dest_conn, uid, tag, ver)

# for a given user, 
def check_individual_user(source_conn, dest_conn, uid, tag, ver):

    if uid == "inktank:greg":
      return

    (ret, out) = rgwadmin_rest(dest_conn, ['metadata', 'userget'], {"key":uid})

    # if this user does not exist on the destination side, add it
    if (ret == 404 and out()['Code'] == 'NoSuchKey'):
        print 'user: ', uid, ' missing from the remote side. Adding it'
        add_user_to_remote(source_conn, dest_conn, uid, tag, ver)
    else: # if the user exists on the remote side, ensure they're the same version
        dest_ver = out()['ver']['ver']
        if dest_ver != ver:
            print 'uid: ', uid, ' local_ver: ', ver, ' != dest_ver: ', dest_ver, ' UPDATING'
            update_remote_user(source_conn, dest_conn, uid, tag, ver)
        else:
            print 'uid: ', uid, ' local_ver: ', ver, ' == dest_ver: ', dest_ver 

# metadata changes are grouped into shards based on [ uid | tag ] TODO, figure this out
def process_source_shard(source_conn, dest_conn, log_entry_list):
    perUserVersionDict = {}
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
        check_individual_user(source_conn, dest_conn, uid, tag, perUserVersionDict[key])

def rgwadmin_rest(connection, cmd, params=None, headers=None, raw=False, data=None):
    log.info('radosgw-admin-rest: %s %s' % (cmd, params))
    put_cmds = ['create', 'link', 'add', 'userput']
    post_cmds = ['unlink', 'modify']
    delete_cmds = ['trim', 'rm', 'process']
    get_cmds = ['check', 'info', 'show', 'list', 'get', 'userget']

    bucket_sub_resources = ['object', 'policy', 'index']
    user_sub_resources = ['subuser', 'key', 'caps']
    #zone_sub_resources = ['pool', 'log', 'garbage']
    zone_sub_resources = ['pool', 'garbage']
    mdlog_sub_resources= ['mdlog']

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
            if len(cmd) == 2 and cmd[1]=='list':
                return 'log/'+cmd[1], ''
            else:
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

def main():

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

  # now get the metadata list from the source RGW
  #(ret, out) = rgwadmin_rest(conn_source, ['user', 'info'], {"uid":"buck"})
  #print 'ret: ', ret
  #print 'user_id: ', out()['user_id']
  #print 'display_name: ', out()['display_name']

  #(ret, out) = rgwadmin_rest(conn_source, ['metadata', 'list'], {"":""})
  #print 'ret: ', ret
  #print 'out: ', out()

  #print 'foo 7'

  (ret, out) = rgwadmin_rest(conn_source, ['metadata', 'list', 'user'],)
  print 'ret: ', ret
  print 'out: ', out()

  (ret, out) = rgwadmin_rest(conn_source, ['log', 'list'], {"type":"metadata"})
  print 'ret: ', ret
  print 'out: ', out()
  numObjects = out()['num_objects']
  print 'We have ', numObjects, ' master objects to check'

  for i in xrange(numObjects):
    (ret, out) = rgwadmin_rest(conn_source, ['log', 'list'], {"type":"metadata", "id":i})
    if (out()): 
        process_source_shard(conn_source, conn_dest, out())

  (ret, out) = rgwadmin_rest(conn_dest, ['log', 'list'], {"type":"metadata"})
  print 'ret: ', ret
  print 'out: ', out()
  numObjects = out()['num_objects']
  print 'We have ', numObjects, ' dest objects to check'

  #(ret, out) = rgwadmin_rest(conn_source, ['metadata', 'userget'], {"key":"buck"})
  #print 'ret: ', ret
  #print 'out: ', out()

  #(ret, out) = rgwadmin_rest(conn_dest, ['metadata', 'userget'], {"key":"buck"})
  #print 'ret: ', ret
  #print 'out: ', out()


if __name__ == '__main__':
  main()
