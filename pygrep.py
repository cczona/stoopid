#
#
#
#

import sys
import bz2
import gzip
import re

# argv[1] = input
# argv[2] = output
# argv[3] = compress type (plain, bz2, gzip)
# argv[4] = filename

filename = sys.argv[1]

expressions = [ 'findme' ]

re_compiled = [ re.compile(x) for x in expressions ]

f = {'gzip': gzip.GzipFile, 'bz2': bz2.BZ2File}.get(sys.argv[3], open)(filename, 'rb')
f_out = open(sys.argv[2], 'a')

line = 0

for l in f:
    line += 1
    for r in re_compiled:
        if r.search(l):
            f_out.write('%s@%d: %s' % (sys.argv[4], line, l))

f_out.close()
