#!/usr/bin/env python

import pymysql
from app import app
from config import mysql
from flask import jsonify
from flask import flash, request
import json
import ast
import datetime
		
@app.route('/register', methods=['POST'])

def usrRegister():
	try:
		_json = request.json
		userLevel = _json['userLevel']
		nickname = _json['nickname']
		userStatus = _json['userStatus']
		userUniqueId = _json['userUniqueId']
        data = dict()
		if userLevel and nickname and userStatus and userUniqueId and request.method == 'POST':			
			sqlQuery = "INSERT INTO userregistration(userLevel, nickname, userStatus, userUniqueId) VALUES(%s, %s, %s, %s)"
			bindData = (userLevel, nickname, userStatus, userUniqueId)
			conn = mysql.connect()
			cursor = conn.cursor()
			cursor.execute(sqlQuery, bindData)
			userId = cursor.lastrowid
			conn.commit()
            data["response"] = "Success"
            data["userId"] = userId
			respone = json.loads(data)
			return respone
		else:
			return not_found()
	except Exception as e:
		respone = jsonify('Failed')
		respone.status_code = 200
		print(e)
	finally:
		cursor.close() 
		conn.close()
###########################################################################################################################
@app.route('/userDetails', methods=['GET'])

def userDetails():
	try:
		conn = mysql.connect()
		cursor = conn.cursor(pymysql.cursors.DictCursor)
		cursor.execute("SELECT userLevel, userUniqueId, nickname, userStatus, gender, birthday, location FROM userregistration where userId="+str(request.args['uid']))
		userData = cursor.fetchall()
        userDataValues = dict()
		if len(userData)>0:
			userData=ast.literal_eval(json.dumps(userData))
            userDataValues["userLevel"] = userData[0]['userLevel']
            userDataValues["userUniqueId"] = userData[0]['userUniqueId']
            userDataValues["nickname"] = userData[0]['nickname']
            userDataValues["userStatus"] = userData[0]['userStatus']
            userDataValues["gender"] = userData[0]['gender']
            userDataValues["birthday"] = userData[0]['birthday']
            userDataValues["location"] = userData[0]['location']
		else:
            userDataValues["userLevel"] = 1
            userDataValues["userUniqueId"] = "1a1a1a"
            userDataValues["nickname"] = "n/a"
            userDataValues["userStatus"] = 0
            userDataValues["gender"] = "n/a"
            userDataValues["birthday"] = "n/a"
            userDataValues["location"] = "n/a"
		response = json.loads(userDataValues)
		return response
	except Exception as e:
		print(e)
	finally:
		cursor.close() 
		conn.close()
###########################################################################################################################
@app.route('/statDaily', methods=['GET'])

def statDaily():
	try:
		conn = mysql.connect()
		cursor1 = conn.cursor(pymysql.cursors.DictCursor)
		cursor1.execute("SELECT userId, pointsEarned, day, week, stepsWalked, timeId FROM userstat where userId="+str(request.args['uid'])+" order by userStatId LIMIT 7")
		statData = cursor1.fetchall()
		cursor3 = conn.cursor(pymysql.cursors.DictCursor)
        data = dict()
		if len(statData)>0:
			for i in range(len(statData)):
                eachdata = dict()
				statData[i]=ast.literal_eval(json.dumps(statData[i]))
				cursor3.execute("SELECT day,week, month FROM timedetails where timeid in ("+str(statData[i]['timeId'])+")")
				timeData = cursor3.fetchall()
				timeData[0]=ast.literal_eval(json.dumps(timeData[0]))
                eachdata["userId"] = statData[i]['userId']
                eachdata["pointsEarned"] = statData[i]['pointsEarned']
                eachdata["week"] = statData[i]['week']
                eachdata["day"] = str(timeData[0]['day'])+"."+str(timeData[0]['month'])
                eachdata["stepsWalked"] = statData[i]['stepsWalked']
                data["day"+str(statData[i]['day'])] = eachdata
		else:
			for i in range(7):
                eachdata = dict()
                eachdata["userId"] = 0
                eachdata["pointsEarned"] = 0
                eachdata["week"] = 0
                eachdata["day"] = 0
                eachdata["stepsWalked"] = 0
                data["day"+str(i+1) = eachdata
		response = json.loads(data)
		return response
	except Exception as e:
		print(e)
	finally:
		cursor1.close() 
		cursor3.close() 
		conn.close()

######################################################################################################################
@app.route('/statMonth', methods=['GET'])

def statMonth():
	try:
		conn = mysql.connect()
		cursor1 = conn.cursor(pymysql.cursors.DictCursor)
		cursor1.execute("SELECT stepsWalked, pointsEarned, timeId FROM userstat where userId="+str(request.args['uid']))
		timeData = cursor1.fetchall()
		if len(timeData)>0:
			timeId = ""
			lmonth=[]
			for  i in range(len(timeData)):
				timeId = timeId + str(timeData[i]['timeId']) + ','
			timeId = timeId[:-1]
			cursor2 = conn.cursor(pymysql.cursors.DictCursor)
			cursor2.execute("SELECT distinct(month), year FROM timedetails where timeId in ("+timeId+") order by year LIMIT 7")
			monthData = cursor2.fetchall()
			cursor3 = conn.cursor(pymysql.cursors.DictCursor)
			for i in range(len(monthData)):
				if monthData[i] not in lmonth:
					lmonth.append(monthData[i])
			input = ''
			m = int(lmonth[0]['month'])
			y = int(lmonth[0]['year'])
			if len(lmonth)<7:
				for i in range(7-len(lmonth)):
					dt = datetime.datetime(y, m, 2)
					prev = dt.replace(day=1) - datetime.timedelta(days=1)
					input = "\"month"+str(7-len(lmonth)-i)+"\": {\"pointsEarned\":0, \"stepsWalked\":0, \"month\": "+str(prev.month)+"}," + input
					if m==1:
						m=12
						y=y-1
					else:
						m=m-1
				input = "{" + input
			else:
				input = "{" + input
			for i in range(len(lmonth)):
				cursor3.execute("select sum(pointsEarned) as pointsEarned, sum(stepsWalked) as stepsWalked FROM userstat where timeId in (Select timeid from timedetails where month in ('"+str(lmonth[i]['month'])+"')) ")
				monthStatData = cursor3.fetchall()
				js="\"month"+str(i+8-len(lmonth))+"\": {\"stepsWalked\":"+str(monthStatData[0]['stepsWalked'])+",\"pointsEarned\":"+str(monthStatData[0]['pointsEarned'])+", \"month\": "+str(lmonth[i]['month'].lstrip('0'))+"}"
				input=input+js+","
			input=input[:-1]
			input=input+"}"
		else:
			for i in range(7):
				js="\"month"+str(i+1)+"\": {\"stepsWalked\":0,\"pointsEarned\":0, \"month\": 0}"
				input=input+js+","
			input=input[:-1]
			input=input+"}"
		response = json.loads(input)
		return response
	except Exception as e:
		print(e)
	finally:
		cursor1.close()
		cursor2.close()
		conn.close()
######################################################################################################################
@app.route('/statWeek', methods=['GET'])

def statWeek():
	try:
		conn = mysql.connect()
		cursor1 = conn.cursor(pymysql.cursors.DictCursor)
		cursor1.execute("SELECT stepsWalked, pointsEarned, timeId FROM userstat where userId="+str(request.args['uid']))
		timeData = cursor1.fetchall()
		if len(timeData)>0:
			timeId = ""
			for  i in range(len(timeData)):
				timeId = timeId + str(timeData[i]['timeId']) + ','
			timeId = timeId[:-1]
			cursor2 = conn.cursor(pymysql.cursors.DictCursor)
			cursor2.execute("SELECT distinct(week), year, month FROM timedetails where timeId in ("+timeId+") order by year")
			weekData = cursor2.fetchall()
			cursor3 = conn.cursor(pymysql.cursors.DictCursor)
			lweek=[]
			for i in range(len(weekData)):
				if weekData[i] not in lweek and len(lweek)<=7:
					lweek.append(weekData[i])
			for i in range(len(lweek)):
				lweek=sorted(lweek, key = lambda k:k['month'])
				for j in range(1, len(lweek)):
					lweek=sorted(lweek, key = lambda k:k['year'])
			input = ''
			c = 7
			if len(lweek)<7:
				for i in range(7-len(lweek)):
					day = str(lweek[0]['week'])+'.'+str(lweek[0]['year'])
					dt = datetime.datetime.strptime(day, '%d.%m.%Y')
					start = dt - datetime.timedelta(days=dt.weekday())
					end = start - datetime.timedelta(days=c)
					print ("previous week: "+ str(end.strftime('%d.%m')))
					pv = end.strftime('%d.%m')
					input = "\"week"+str(7-len(lweek)-i)+"\": {\"pointsEarned\":0, \"stepsWalked\":0, \"week\": "+str(pv.lstrip('0'))+"}," + input
					c=c+7
				input = "{" + input
			else:
				input = "{" + input
			for i in range(len(lweek)):
				cursor3.execute("select sum(pointsEarned) as pointsEarned, sum(stepsWalked) as stepsWalked FROM userstat where timeId in (Select timeid from timedetails where week in ('"+str(lweek[i]['week'])+"')) ")
				weekStatData = cursor3.fetchall()
				js="\"week"+str(i+8-len(lweek))+"\": {\"stepsWalked\":"+str(weekStatData[0]['stepsWalked'])+",\"pointsEarned\":"+str(weekStatData[0]['pointsEarned'])+", \"week\": "+str(lweek[i]['week'].lstrip('0'))+"}"
				input=input+js+","
			input=input[:-1]
			input=input+"}"
		else:
			for i in range(7):
				js="\"week"+str(i+1)+"\": {\"stepsWalked\":0,\"pointsEarned\":0, \"week\":0}"
				input=input+js+","
			input=input[:-1]
			input=input+"}"
		response = json.loads(input)
		return response
	except Exception as e:
		print(e)
	finally:
		cursor1.close()
		cursor2.close()
		cursor3.close()
		conn.close()

######################################################################################################################
@app.route('/statYear', methods=['GET'])

def statYear():
	try:
		conn = mysql.connect()
		cursor1 = conn.cursor(pymysql.cursors.DictCursor)
		cursor1.execute("SELECT year, stepsWalked, pointsEarned FROM statyear where userId="+str(request.args['uid']))
		statData = cursor1.fetchall()
		cursor2 = conn.cursor(pymysql.cursors.DictCursor)
		cursor2.execute("SELECT stepsWalked,pointsEarned FROM currentdaystats where userId="+str(request.args['uid']))
		dailyData = cursor2.fetchall()
		input = "{"
		for i in range(len(statData)):
			statData[i]=ast.literal_eval(json.dumps(statData[i]))
			js="\"year"+str(statData[i]['year'])+"\": {\"stepsWalked\":"+str(statData[i]['stepsWalked'])+",\"pointsEarned\":"+str(statData[i]['pointsEarned'])+"}"
			input=input+js+","
		input=input[:-1]
		input=input+",\"dailyPointsEarned\":"+str(dailyData[0]['pointsEarned'])+",\"dailyStepsWalked\":"+str(dailyData[0]['stepsWalked'])+"}"
		response = json.loads(input)
		return response
	except Exception as e:
		print(e)
	finally:
		cursor1.close()
		cursor2.close()
		conn.close()




######################################################################################################################


@app.route('/getRewards', methods=['GET'])

def getRewards():
	try:
		conn = mysql.connect()
		cursor = conn.cursor(pymysql.cursors.DictCursor)
		cursor.execute("SELECT id, brand, rate, url FROM rewards")
		rewardData = cursor.fetchall()
		input = "{\"rewards\": ["
        input = dict()
        rewards = []
		for i in range(len(rewardData)):
            eachReward = dict()
			rewardData[i]=ast.literal_eval(json.dumps(rewardData[i]))
            eachReward["id"] = rewardData[i]['id']
            eachReward["brand"] = rewardData[i]['brand']
            eachReward["rate"] = rewardData[i]['rate']
            eachReward["uri"] = rewardData[i]['uri']
            rewards.append(eachReward)
        input["rewards"] = rewards
		response = json.loads(input)
		return response
	except Exception as e:
		print(e)
	finally:
		cursor.close() 
		conn.close()

###########################################################################################################################
@app.route('/homeRequest', methods=['GET'])

def homeRequest():
	try:
		conn = mysql.connect()
		cursor1 = conn.cursor(pymysql.cursors.DictCursor)
		cursor1.execute("SELECT userLevel, profileLevelPercentage, nickname FROM userregistration where userId="+str(request.args['uid']))
		userData = cursor1.fetchall()
		if len(userData)>0:
			userLevelId = userData[0]['userLevel']
			profileLevelPercentage = userData[0]['profileLevelPercentage']
			nickname = userData[0]['nickname']
		else:
			userLevelId = 1
			profileLevelPercentage = '10'
			nickname = "\"n/a\""
		cursor2 = conn.cursor(pymysql.cursors.DictCursor)
		#dt = datetime.datetime.today()
		#today = dt.strftime('%d%m%Y')
		today = '19052020'
		cursor2.execute("SELECT stepsWalked,pointsEarned FROM userstat where timeId = "+today)
		dailyData = cursor2.fetchall()
        data = dict()
		if len(dailyData)>0:
			if userLevelId == 1:
				userLevel = 'Beginner'
			elif userLevelId == 2:
				userLevel = 'Medium'
			else:
				userLevel = 'Master' 
            data["userClass"] = userLevel
            data["profileLevelPcnt"] = profileLevelPercentage
            data["stepsWalked"] = dailyData[0]['stepsWalked']
            data["pointProg"] = dailyData[0]['pointsEarned']
            data["stepsGoal"] = 10000
		else:
            data["userClass"] = userLevel
            data["profileLevelPcnt"] = profileLevelPercentage
            data["stepsWalked"] = 1
            data["pointProg"] = 1
            data["stepsGoal"] = 10000
		response = json.loads(data)
		return response
	except Exception as e:
		print(e)
	finally:
		cursor1.close() 
		cursor2.close()
		conn.close()

################################################################################################################################
@app.route('/challengeList', methods=['GET'])

def challengeList():
	try:
		conn = mysql.connect()
		cursor1 = conn.cursor(pymysql.cursors.DictCursor)
		cursor1.execute("SELECT achievements_activity_id, achievement_id, progress from achievements_activity where user_id="+str(request.args['uid'])+" order by achievement_id")
		achData = cursor1.fetchall()
		print("achdata: "+str(achData))
		cursor2 = conn.cursor(pymysql.cursors.DictCursor)
		cursor2.execute("SELECT achievement_id, name, description, target from achievements")
		achList = cursor2.fetchall()
		progress = 0
		challengeDetails = []
        input = dict()
        challenges = []
		for i in range(len(achList)):
			challengeDetails.append(achList[i])
		if len(achData)==0:
			for i in range(len(achList)):
                eachChallenge = dict()
                eachChallenge["id"] = challengeDetails[i]['achievement_id']
                eachChallenge["name"] = challengeDetails[i]['name']
                eachChallenge["description"] = challengeDetails[i]['description']
                eachChallenge["target"] = challengeDetails[i]['target']
                eachChallenge["progress"] = progress
                challenges.append(eachChallenge)
		else:
			for i in range(len(challengeDetails)):
				for j in range(len(achData)):
					if achList[i]['achievement_id'] == achData[j]['achievement_id']:
						challengeDetails[i]['progress'] = str(achData[j]['progress'])
						continue
					else: 
						challengeDetails[i]['progress'] = str(0)

			for i in range(len(challengeDetails)):
                eachChallenge = dict()
                eachChallenge["id"] = challengeDetails[i]['achievement_id']
                eachChallenge["name"] = challengeDetails[i]['name']
                eachChallenge["description"] = challengeDetails[i]['description']
                eachChallenge["target"] = challengeDetails[i]['target']
                eachChallenge["progress"] = challengeDetails[i]['progress']
                challenges.append(eachChallenge)
           input["challengeValues"] = challenges
		response = json.loads(input)
		return response
	except Exception as e:
		print(e)
	finally:
		cursor1.close() 
		cursor2.close()
		conn.close()

##################################################################################################################################
@app.route('/updateUserDetails', methods=['PUT'])

def updateUserDetails():
	_json = request.json
	nickname = _json["nickname"]
	userId = _json["userId"]
	if nickname and userId:
		try:
			sqlQuery = "update userregistration set nickname=%s where userId=%s"
			bindData = (nickname, userId)
			conn = mysql.connect()
			cursor = conn.cursor()
			cursor.execute(sqlQuery, bindData)
			conn.commit()
			js["response"]: "Success"
			response = json.loads(js)
			return response
		except Exception as e:
			response = json.loads("{\"response\": \"Failed\"}")
			return response
		finally:
			cursor.close()
			conn.close()
###########################################################################################################################
if __name__ == "__main__":
	app.run(host='192.168.0.103', port=5000, debug=True)
    