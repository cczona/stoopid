#!/python

import os
import sys

for filename in os.listdir('/dev/in'):
    with open(os.path.join('/dev/in', filename), 'r') as f:
        sys.stdout.write(f.read())
