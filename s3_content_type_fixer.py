#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Fixes wrong Content-Type on Amazon S3 services.
"""

import argparse
import sys
import mimetypes
import multiprocessing
from boto3.session import Session

BLOCK_TIME = 60 * 60

def find_matching_files(bucket, prefixes):
    """
    Returns a set of files in a given S3 bucket that match the specificed file
    path prefixes
    """
    return set(key for prefix in prefixes for key in bucket.objects.filter(Prefix=prefix))

def get_bucket(access_key, secret_key, bucket):
    """Gets an S3 bucket"""
    session = Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name='ap-northeast-1')
    s3r = session.resource("s3")
    bucket = s3r.Bucket(bucket)
    return bucket

def check_headers(bucket, queue, verbose, dryrun):
    """
    Callback used by sub-processes to check the headers of candidate files in
    a multiprocessing queue
    """

    while True:
        try:
            key_name = queue.get(BLOCK_TIME)
        except :
            break

        if key_name is None:
            break

        key = bucket.Object(key_name)

        if not key:
            print >> sys.stderr, "%s: Could not lookup" % key.key
            continue

        if key.key.endswith('/'): # skip directories
            continue

        content_type = key.content_type
        expected_content_type, _ = mimetypes.guess_type(key.key)

        if not expected_content_type:
            print >> sys.stderr, "%s: Could not guess content type" % key.key
            continue

        if content_type == expected_content_type:
            if verbose:
                print "%s: Matches expected content type" % key.key
        else:
            print "%s: Current content type (%s) does not match expected (%s); fixing" \
                % (key.key, content_type, expected_content_type)
            if not dryrun:
                metadata = key.metadata

                # Because metadata['Content-Type'] will be shown as 'x-amz-meta-content-type'
                # in console. This is not what we want.
                if 'Content-Type' in metadata:
                    metadata["Content-Type"] = expected_content_type

                if key.content_disposition:
                    metadata["Content-Disposition"] = key.content_disposition

                key.copy_from(
                    Bucket=key.bucket_name,
                    Key=key.key,
                    CopySource=key.bucket_name + '/' + key.key,
                    MetadataDirective="REPLACE",
                    Metadata=metadata,
                    ContentType=expected_content_type)

def main():
    """
    main function
    """
    parser = argparse.ArgumentParser(description="Fixes the content-type of assets on S3")

    parser.add_argument("--access-key", "-a", type=str, required=True, help="The AWS access key")
    parser.add_argument("--secret-key", "-s", type=str, required=True, help="The AWS secret key")
    parser.add_argument("--bucket", "-b", type=str, required=True, help="The S3 bucket to check")
    parser.add_argument("--prefixes", "-p", type=str, default=[""], required=False, nargs="*",
                        help="File path prefixes to check")
    parser.add_argument("--workers", "-w", type=int, default=4, required=False,
                        help="The number of workers")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Verbose output")
    parser.add_argument("--dryrun", "-d", action="store_true", default=False, required=False,
                        help="Add this for a dry run (don't change any file)")

    args = parser.parse_args()
    queue = multiprocessing.Queue()
    processes = []
    bucket = get_bucket(args.access_key, args.secret_key, args.bucket)

    # Start the workers
    for _ in xrange(args.workers):
        proc = multiprocessing.Process(
            target=check_headers, args=(bucket, queue, args.verbose, args.dryrun))
        proc.start()
        processes.append(proc)

    # Add the items to the queue
    for key in find_matching_files(bucket, args.prefixes):
        queue.put(key.key)

    # Add None's to the end of the queue, which acts as a signal for the
    # proceses to finish
    for _ in xrange(args.workers):
        queue.put(None)

    for proc in processes:
        # Wait for the processes to finish
        try:
            proc.join()
        except KeyboardInterrupt:
            pass

if __name__ == "__main__":
    main()
