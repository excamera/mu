#!/usr/bin/env python
import json
import argparse


_parser = argparse.ArgumentParser()
_parser.add_argument("-c", "--config", help="path to config file", type=argparse.FileType('r'),
                     default=open("pipeline_conf.json", 'r'))
_args, _ = _parser.parse_known_args()

settings = json.load(_args.config)
