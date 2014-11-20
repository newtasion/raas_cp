#!/usr/bin/python
import argparse
from celery import result

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', '--revoke', dest='revoke', help="revoke a task")
    args = parser.parse_args()

    if args.revoke:
        result.AsyncResult(args.revoke).revoke()

