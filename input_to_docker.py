#!/usr/bin/python
import os
import json
import ast
import os
import subprocess
import sys
import time
import fnmatch
import psycopg2
from dateutil import tz
import datetime
import psutil
from kafka import KafkaConsumer

# This is a program written to calucate the parameters for a docker container_code
# The parameters are for processing an input video file into specified output formats and forms.


max_contaners = 7
extensions = ('.mp4', '.avi', '.wmv', '.mkv', '.mov')
wait_timeout = 5
image_name="encoder:eachcore"

consumer = KafkaConsumer('bitryt',bootstrap_servers='192.168.1.9:9092')

def getStitchParam(data):
	stitchingStatus=data['stitchingStatus']
	if stitchingStatus == 'false':
		stitchName = 0
		stitchLocation = 0
		stitchingStatus = 0
	elif stitchingStatus == 'true':
		stitchingStatus = 1
		stitchName = ""
		stitchLocation = ""
		for data in range(len(data['stitchingInfo'])): 
			sFileName = data['stitchingInfo'][data]['fileName']
			sFileLocation = data['stitchingInfo'][data]['fileLocation']
			sFilePosition = data['stitchingInfo'][data]['position']
			sFileIndex = data['stitchingInfo'][data]['index']

			if fnmatch.fnmatch(sFilePosition,'PRE'):
				stitchName = stitchName+"-1+"+stitchingFileName+"|"
			elif fnmatch.fnmatch(sFilePosition,'POST'):
				stitchName = stitchName+"1+"+sFileName+"|"
			stitchLocation = stitchLocation+"/tmp/workdir/Uploaded/"+sFileLocation+"|"
		stitchName = stitchName[:-1]
		stitchLocation = stitchLocation[:-1]
	return stitchName, stitchLocation

def getWatermarkParam(data):
	watermarkStatus = data['waterMark']['watermarkStatus']
	watermarkParam = ""
	if watermarkStatus=='false':
		watermarkParam = watermarkParam+"0,0,0"
	elif watermarkStatus=='true':
		watermarkParam = watermarkParam+str(1)
		wHAlign = ""
		wVAlign = ""
		wImUrl = ""
		for wValue in range(len(data['waterMark']['waterMarkDetails'])):
			hAlignment = data['waterMark']['waterMarkDetails'][wValue]['hAlignment']
			hOffset = data['waterMark']['waterMarkDetails'][wValue]['hOffset']
			if hAlignment=='0':
				hAlignment = hOffset
			else:
				hAlignment = hAlignment+"-"+str(hOffset)
			vAlignment = data['waterMark']['waterMarkDetails'][wValue]['vAlignment']
			verticalOffset = data['waterMark']['waterMarkDetails'][wValue]['vOffset']
			if vAlignment=='0':
				vAlignment = vOffset
			else:
				vAlignment=vAlignment+"-"+str(vOffset)					
			wFolder = data['waterMark']['waterMarkDetails'][wValue]['mediaLocation']
			wFileName = data['waterMark']['waterMarkDetails'][wValue]['waterMarkFileName']
			wFile = '/tmp/workdir/Uploaded/'+watermarkFolder+watermarkFileName
			wHAlign = wHAlign+hAlignment+"|"
			wVAlign = wVAlign+vAlignment+"|"
			wImUrl = wImUrl+wFile+"|"
		wHAlign = wHAlign[:-1]
		wVAlign = wVAlign[:-1]
		wImUrl = wImUrl[:-1]
		watermarkParam = watermarkParam+",\""+str(wHAlign)+"\",\""+str(wVAlign)+"\",\""+str(wImUrl)+"\""
	
	return watermarkParam

def getPresetParam():
	preset_id = data['preset'][pItem]['mediaPresetId']
	width = data['preset'][pItem]['width']
	height = data['preset'][pItem]['height']
	videoCodec = data['preset'][pItem]['videoCodec']
	aspectRatio = data['preset'][pItem]['aspectRatio']
	minRate = data['preset'][pItem]['minRate']
	bufferSize = data['preset'][pItem]['bufferSize']
	maxRate = data['preset'][pItem]['maxRate']
	preset = data['preset'][pItem]['preset']
	videoBitrate = data['preset'][pItem]['videoBitrate']
	profile = data['preset'][pItem]['profile']
	level = data['preset'][pItem]['level']
	audioBitrate = data['preset'][pItem]['audioBitrate']
	audioCodec = data['preset'][pItem]['audioCodec']
	encodeFormat = data['preset'][pItem]['mediaContainer']
	return str(preset_id)+","+str(width)+","+str(height)+","+str(videoCodec)+","+str(minRate)+","+str(bufferSize)+","+str(maxRate)+","+str(preset)+","+str(videoBitrate)+","+str(profile)+","+str(level)+","+str(audioCodec)+","+str(audioBitrate)+","+str(aspectRatio)+","+str(encodeFormat)

def is_container_avail():
     a = os.popen("docker ps | awk '{print $1}' | grep -v CONTAINER | wc -l").read()
     c_count = a.strip("\n")
     if int(c_count) < max_contaners:
        return True
     else:
        return False

for msg in consumer:
	db = psycopg2.connect( host = hostname,user = username,password = password,dbname = dbname )
	try:
		ast.literal_eval(msg.value) 
		
		data = json.loads(msg.value)

		jobId= data['jobId']	
		video= data['mediaInput']['inputMediaName']
		folder= "/home/fayas/Documents/Docker/dynamic"
		outputVideo= data['mediaInput']['outputMediaPrefix']
		outputMediaLocation= data['mediaInput']['outputMediaLocation']
		mediaId= data['mediaId']
		baseUrl= data['baseUrl']
		inputStorageName= data['mediaInput']['inputStorageName']
		inputMediaName= data['mediaInput']['inputMediaName']
		inputMedia= inputStorageName+inputMediaName
		userId= data['userId']
		mount_dir=baseUrl.replace('Uploaded/','')
		userPreferredDestinationStatus= data['userPreferredDestinationStatus']
		if userPreferredDestinationStatus == 'true':
			print "output destination is selected"
			outputType=data['userPreferredDestinationPath']['pathType']
			print "outputType"+str(outputType)
			if outputType == 1:
####Here value for directory is saved under user_name since user_name is the first argument passed
				username = data['userPreferredDestinationPath']['pathName']
				username = username.replace(mount_dir,"")
				username = '/tmp/workdir/'+str(username)
				directory = 0
				password = 0
				hostname =0
				port = 0

				username = data['userPreferredDestinationPath']['userName']
				password = data['userPreferredDestinationPath']['password']
				directory = data['userPreferredDestinationPath']['pathName']

			if outputType == 2:
				hostname = data['userPreferredDestinationPath']['bucketName']
				port = data['userPreferredDestinationPath']['region']
			elif outputType == 3:
				hostname = data['userPreferredDestinationPath']['hostName']
				port = data['userPreferredDestinationPath']['port']
		else:
			outputType = 0
			username = 0
			password = 0
			directory = 0
			hostname = 0
			port = 0
		montageStatus = data['montageStatus']
		if montageStatus == 'true':
			montageStatusValue = 1
		else:
			montageStatusValue = 0

		#old_stdout = sys.stdout
		#log_file = open(folder+"/Transcoded/"+str(outputVideo)+".log","w")
		if is_container_avail():
			t = 1
		else:
			print "Inside Core Count Else"
			while str(is_container_avail())=='False':
				print 'Contaner limit exceded, waiting ...'
				time.sleep(wait_timeout)
			t=1
		if t==1:
			stitchData = getStitchParam(data)

#######################fetching watermark parameters
			watermarkData = getWatermarkParam(data)
			tflow=0

############Fetching presets
			for pItem in range(len(data['preset'])):
				cpu_utilization = psutil.cpu_percent(interval=5, percpu=True)
				count=0
				for cpu_core in cpu_utilization:
						if int(cpu_core)<10:
							cpu_number = count
							print "CPU Number"+str(cpu_number)
							break
						else:
							count=count+1
				docker_call="docker run --privileged -u $(id -u $(whoami)) --cpuset-cpus="+str(cpu_number)+" -d -v \""+mount_dir+":/tmp/workdir\" "+image_name+" "+str(video)+" \""+str(stitchingStatusValue)+","+str(stitchName)+","+str(stitchLocation)+"\" /tmp/workdir/"+str(inputMedia)+" "+str(mediaId)+" "+str(jobId)+" "+str(userId)+" "+str(outputVideo)+" /tmp/workdir/ "+str(outputType)+","+str(username)+","+str(password)+","+str(port)+","+str(hostname)+","+str(directory)+" "+str(cpu_number)+" "+str(montageStatusValue)+" "+watermarkParameter
############Thumbnail check
				if tflow==0:
					tStatus = data['thumbnail']['thumbnailStatus']
					if thumbnailStatus=='false':
						docker_call = docker_call+" 0,0 "
					elif tStatus=='true':
						docker_call = docker_call+" "+str(1)+",\""
						for tValue in range(len(data['thumbnail']['thumbnailDetails'])):
							tName = data['thumbnail']['thumbnailDetails'][tValue]['thumbnailFileName']
							docker_call = docker_call+tName+"|"
						docker_call=docker_call[:-1]+"\""
						for tValue in range(len(data['thumbnail']['thumbnailDetails'])):
							tTimeFrame = data['thumbnail']['thumbnailDetails'][tValue]['timeFrame']
							docker_call = docker_call+","+str(tTimeFrame)
						docker_call = docker_call+" "
					tflow = tflow+1
				else:
					docker_call=docker_call+" 0,0 "
############thumbnail check completed				

############preset values stored in variables
				preset = getPresetParam(data)
				docker_call=docker_call+str(len(data['preset']))+" "+"\""+preset+"\" "
				os.system(docker_call)
############reset montageStatusValue to stop creating again if enabled
				montageStatusValue = 0
 
			now = datetime.datetime.now()
			var=now.strftime("%Y-%m-%d %H:%M:%S")
			from_zone = tz.gettz('UTC')
			to_zone = tz.gettz('Asia/Kolkata')
			utc = datetime.datetime.strptime(var, '%Y-%m-%d %H:%M:%S')
			time_now = utc.replace(tzinfo=from_zone)
			try:
				sql="update public.\"EncoderJobs\" set \"start_time\" = '"+str(time_now)+"', \"job_status_id\" = '2', \"container_code\" = '"+str(cpu_number)+"' where \"job_id\" = '"+str(jobId)+"' ;"
				print str(sql)
				cur = db.cursor()
				cur.execute(sql)
				db.commit()
			except Exception, e:
				print "Exception: "+str(e)
			finally:
				cur.close()
				db.close()

	except Exception as e:
		print "Error in Message:"+str(e)
		file = open("Encoder-error.log","a")
		file.write(str(e)+"\n")
		file.close()
