#!/usr/bin/python

from REST_metadata_syncher import REST_metadata_syncher

# this instantiates a MetadataSyncher and kicks off a synch of all shards in a repo
# hard-coded only for testing
access_key_source = '85OKY95S2US1LB4Z4ZYI' #buck
secret_key_source = 'wy4DkpcGG+fAuOMfYPYAI37P7Qe2kg+oQpZIqByk'
host_source = 'rgw-n1'
access_key_dest = '95OKY95S2US1LB4Z4ZYI'
secret_key_dest = 'zy4DkpcGG+fAuOMfYPYAI37P7Qe2kg+oQpZIqByl'
host_dest = 'rgw-n2'

if __name__ == '__main__':
    syncher = REST_metadata_syncher(access_key_source, secret_key_source, host_source,
                                    access_key_dest, secret_key_dest, host_dest)

    # sync all the data
    syncher.sync_all_shards()
