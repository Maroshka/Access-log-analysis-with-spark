"""Object Oriented Programmine"""
###This is a simple script to get started with spark ###
import os, sys
import re
import subprocess
import datetime
import plotly.plotly as py
import plotly.graph_objs as go
import plotly.tools as tls
tls.set_credentials_file(username='maroshka', api_key='njpjllrdy0')

os.environ['SPARK_HOME'] = "/usr/local/spark"
sys.path.append("/usr/local/spark/python")
sys.path.append("/usr/local/spark/python/lib")
from pyspark import SparkContext 
from pyspark import SparkConf, SparkContext
conf = SparkConf()
conf.setMaster("local[4]")
conf.setAppName("My app")
conf.set("spark.executor.memory", "8g")
conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
sc = SparkContext(conf = conf)
months = {'Jan': '01', 'Feb': '02', 'Mar':'03', 'Apr':'04', 'May': '05', 'Jun': '06', 'Jul':'07', 'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov':'11', 'Dec': '12'}
months = sc.broadcast(months)

class OSDataAnalysis(object):

	@staticmethod
	def urpd(line):
		# line = line.replace("\"", "", 10)
		if "::1" not in line:
			reqTime = re.search(r"\[([A-Za-z0-9_]+)(.+)\]", line).group()[1:-1]
			if re.search(r'\"([A-Za-z0-9_]+)(.+)\"', line) != None: 
				request = re.search(r'\"([A-Za-z0-9_]+)(.+)\"', line).group().split('\"')[1]
			else:
				return False
			# request = re.search(r'\"([A-Za-z0-9_]+)(.+)\"', line).group().split('\"')[1]
			date = reqTime[:11]
			date = date[7:]+'-'+months.value[date[3:6]]+'-'+date[0:2]
		
			return (date, request)
		else: 
			return False

	@staticmethod
	def uhpd(line):
		if "::1" not in line:
			reqTime = re.search(r"\[([A-Za-z0-9_]+)(.+)\]", line).group()[1:-1]
			host = re.search(r'(\d+).(\d+).(\d+).(\d+)', line).group()
			date = reqTime[:11]
			date = date[7:]+'-'+months.value[date[3:6]]+'-'+date[0:2]
		
			return (date, host)
		else: 
			return False

	@staticmethod
	def reqsPerHost(line):
		if "::1" not in line:
			reqTime = re.search(r"\[([A-Za-z0-9_]+)(.+)\]", line).group()[1:-1]
			host = re.search(r'(\d+).(\d+).(\d+).(\d+)', line).group()
			if re.search(r'\"([A-Za-z0-9_]+)(.+)\"', line) != None: 
				request = re.search(r'\"([A-Za-z0-9_]+)(.+)\"', line).group().split('\"')[1]
			else:
				return False
			return (host, request)
		else:
			return False

	@staticmethod
	def n404(line):
		if " 404 " in line:
			reqTime = re.search(r"\[([A-Za-z0-9_]+)(.+)\]", line).group()[1:-1]
			date = reqTime[:11]
			date = date[7:]+'-'+months.value[date[3:6]]+'-'+date[0:2]

			return (date, 1)
		else:
			return False

	@staticmethod
	def n500(line):
		if " 500 " in line:
			reqTime = re.search(r"\[([A-Za-z0-9_]+)(.+)\]", line).group()[1:-1]
			date = reqTime[:11]
			date = date[7:]+'-'+months.value[date[3:6]]+'-'+date[0:2]

			return (date, 1)
		else:
			return False

	@staticmethod
	def run(path, method):
		data = sc.textFile(path).cache()
		data = data.map(method).filter(lambda x: x!=False)
		if method == OSDataAnalysis.n404 or method == OSDataAnalysis.n500:
			data = data.reduceByKey(lambda a, b: a + b).cache()
		else:
			data = data.groupByKey().map(lambda a: (a[0], len(set(a[1])))).cache()
		sumd = data.map(lambda a: a[1]).sum()
		avg = sumd / data.count()
		mx = data.max(lambda a: a[1])
		largestTen = data.takeOrdered(10, lambda a: -a[1])
		data = data.collect()
		if method == OSDataAnalysis.uhpd or method == OSDataAnalysis.urpd:
			data.sort(key=lambda x: datetime.datetime.strptime(x[0], '%Y-%m-%d'))
			# data2 = 
			# x = data2.keys()
			# y = data2.values()
			x = [data[i][0] for i in range(0, len(data)-1)]
			y = [data[i][1] for i in range(0, len(data)-1)]
			data2 = [go.Scatter(x=x,y=y)]
			py.iplot(data2)

		print sumd
		return data, mx, largestTen, avg

def main() :

	path = "hdfs://localhost:5431/test/input/sparktest.txt"
	data, mx, largestTen, avg = OSDataAnalysis.run(path, OSDataAnalysis.reqsPerHost)
	print "Length of Data: "+str(len(data))
	print "Maximum value: ", mx
	print "Average: ", avg
	print "Largest ten values: ", largestTen	

if __name__ == '__main__':
	main()