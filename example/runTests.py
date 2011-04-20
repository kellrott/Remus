#!/usr/bin/env python

import glob
import unittest



if __name__ == "__main__":
	for a in glob.glob("test*.py"):
		name = a.replace(".py", "") 
		suite = unittest.TestLoader().loadTestsFromName(name)
		unittest.TextTestRunner().run(suite)
		
