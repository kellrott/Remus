#!/usr/bin/env python

import os
import yaml
from optparse import OptionParser
import imp
import uuid

import remus.flow

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-c", "--config", dest="config",  help="Remus Config File", default=None)
    parser.add_option("-p", "--pipeline", dest="pipeline", help="Remus Pipeline File", default=None)
    parser.add_option("-s", "--submission", dest="submit", help="Remus Pipeline Submission Parameters", default=None)

    options, args = parser.parse_args()
    
    if len(args) and args[0] == "run":
        
        handle = open(options.submit)
        sub = yaml.load(handle.read())
        handle.close()
        
        handle = open(options.config)
        config = yaml.load(handle.read())
        handle.close()
        
        config["_instance"] = str(uuid.uuid4())
        config["_pipeline"] = os.path.basename(options.pipeline).replace(".py", "")
        remus.flow.set_submission(sub)
        remus.flow.set_runInfo(config)
        module = imp.new_module( "runner" )    
        module.__dict__["__file__"] = options.pipeline        
        module.__dict__["__name__"] = "runner"
        execfile(options.pipeline, module.__dict__)
