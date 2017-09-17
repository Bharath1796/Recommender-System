from itertools import groupby
from operator import itemgetter
from collections import OrderedDict

import sys
import datetime
import math
import datetime

from pyspark import SparkContext
sc=SparkContext("local[6]","MapReduce")

userid = sys.argv[1]
#recentT = int(sys.argv[2])

def extract_datetime(ts):      
        return datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

lstPearson =[]

def pearson(n1, n2):
       muln1n2 = []
       den = num = sum_x = sum_y = sum_xy = sumsq_x = sumsq_y = 0
       if(len(n1) < 4):
                return("SSS")                
                      
       for i in range(0, len(n1)):
              muln1n2.append(n1[i] * n2[i])

       sum_x = listsum(n1)
       sum_y = listsum(n2)
       sum_xy = listsum(muln1n2)
       sumsq_x = listsqrsum(n1)
       sumsq_y = listsqrsum(n2)
       muln1n2 = muln1n2 * len(n1)

       num = (len(n1) * sum_xy) - ((sum_x) * (sum_y))
       den = math.sqrt(((len(n1) * sumsq_x) - math.pow(sum_x,2)) * ((len(n1) * sumsq_y) - math.pow(sum_y,2)))

       try:
              return ("%.4f" % (num/den))
             
       except ZeroDivisionError:
              return("Cannot divide by zero")                          

def listsum(numList):
    theSum = 0
    for i in numList:
        theSum = theSum + i
    return theSum

def listsqrsum(numList):
    theSum = 0
    for i in numList:
        theSum = theSum + (i * i)
    return theSum

def cor(v):
    n1=[]
    n2=[]    
    l=len(v) 
     
    for i in range(0,l):
	h = i%2
        if(h == 0):
        	n1.append(v[i])
        else:
        	n2.append(v[i])
    
    	num1 = [float(x) for x in n1]
    	num2 = [float(x) for x in n2]
    r=pearson(num1, num2)
    return(r)

rawData= sc.textFile("/usr/local/spark/mani/100kclus_0_rat.txt").map(lambda x:x.split("\t")).map(lambda x:(x[0],x[1],x[2]))

#rawData= sc.textFile("/usr/local/spark/Sugu/spark/SVD/100kclus_4_rat.txt").map(lambda x:x.split("\t")).map(lambda x:(x[0],x[1],x[2]))
#rawData = raw.sortBy(lambda x:x[3],False)
	
Stime = datetime.datetime.now()

UsrRat = rawData.filter(lambda x:x[0] == userid).map(lambda x:x[1]).collect()

#UsrRat = rawData.filter(lambda x:x[0] == userid).map(lambda x:x[1]).take(recentT)
activeUsrRat = sc.parallelize(UsrRat)

lstRecommend = []
intcnt = activeUsrRat.count()
#intcnt = UsrRat.count()

for g in range(0,intcnt): 
	sug = activeUsrRat.collect()[g]
	#sug = UsrRat.collect()[g]
	print("*******************************************************************",sug)	

	itemii=rawData.filter(lambda x:x[1] == sug).map(lambda x:(x[0],x[2]))

	u1cart = itemii.cartesian(rawData)

	u1match=u1cart.filter(lambda x:x[0][0]==x[1][0]).map(lambda x:(x[1],x[0][1]))

	u1Mov=u1match.map(lambda x:(x[0][1],(x[0][2],x[1])))

	u1MovSort = u1Mov.sortBy(lambda x:int(x[0]),False)

	mrMov = u1MovSort.reduceByKey(lambda x,y:(x+y))

	res1=mrMov.map(lambda x:(x[0],sug,cor(x[1])))


	res2=res1.filter(lambda x:(x[2]!= "SSS") and (x[2]!="Cannot divide by zero") and (x[0]!= sug)).map(lambda x:x).collect()
	res2.sort(key=lambda x: x[2],reverse=True)

	for b in range(0,5):
		if (b<len(res2)):
			lstRecommend.append(res2[b])

sc.stop()
lstRecommend.sort(key=lambda x: x[2],reverse=True)
#etime = datetime.datetime.now()
for t in range(0,len(res2)):
	if ( t < 30):
    		print(res2[t])
    
print("****************************************",UsrRat)

print()
#print("Start time: ",Stime)
#print("End time: ",etime)
