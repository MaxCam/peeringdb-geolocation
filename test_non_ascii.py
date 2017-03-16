# coding: latin-1
from __future__ import unicode_literals
import sys, codecs, locale

#sys.setdefaultencoding('utf8')
print str(sys.stdout.encoding)
sys.stdout = codecs.getwriter(locale.getpreferredencoding())(sys.stdout)
line = "Malmö"
sys.stdout.write(line)
