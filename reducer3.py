#!/python

import os
import sys

for filename in os.listdir('/dev'):
    if filename.startswith('mapper'):
        with open(os.path.join('/dev', filename), 'r') as f:
            for line in f:
                sys.stdout.write('%s: %s' % (filename, line))
