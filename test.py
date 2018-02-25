import os
import csv
with open ('iris_training.csv', 'rU') as csvfile:
    ##next(csvfile, None) # skip header
    plotlist = csv.reader(csvfile, delimiter=',', dialect=csv.excel_tab)
    for i, row in enumerate(plotlist):
       ## print "[%d] %s" % (i, row)
       print (i,row)

