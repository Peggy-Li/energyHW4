import os
def splitFile(filename):
   chunksize = 1000
   fid = 1
   filesmade = []
   with open(filename) as infile:
       f = open('%s%d' %(filename, fid), 'w')
       for i, line in enumerate(infile):
           f.write(line)
           if not i%chunksize:
               f.close()
               os.remove(f.name)
               fid += 1
               f = open('%s%d' %(filename, fid), 'w')
       f.close()
