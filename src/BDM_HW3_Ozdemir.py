import csv
import sys
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext()
spark = SparkSession(sc)
sc



def extractdata(proid, records):
    if proid==0:
        next(records)
#     for row in records:
#         yield (row)
    import csv
    reader = csv.reader(records)
    for row in reader:
        if len(row)==18:
#         if row[0]!='s': # to filter our bad-quality data
            (date,prod,company) = (row[0][:4], row[1].lower(), row[7].lower())
            yield ((prod, date, company), 1)
            
def towrite (_,rows):
    for (year, prod), (t_complaint, t_company, max_percent) in rows:
        if ',' in prod:
            prod='"{}"'.format(prod)
        yield ','.join((prod, year,str(t_complaint), str(t_company), (max_percent)))
        
        
def rdd1():
    sc.textFile(sys.argv[1])\
            .mapPartitionsWithIndex(extractdata)\
            .reduceByKey(lambda x,y: x+y)\
            .map(lambda x: (x[0][:2], (x[1], 1, x[1])))\
            .reduceByKey (lambda x,y:(x[0]+y[0], x[1]+y[1], max(x[2],y[2])))\
            .mapValues(lambda x: (x[0],x[1], int((x[2]/(x[0]))*100)))\
            .sortByKey()\
            .mapPartitionsWithIndex(towrite)\
            .saveAsTextFile(sys.argv[2])

                        
if __name__=='__rdd1__':
    rdd1()