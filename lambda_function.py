from dateutil import tz
import datetime
import dateutil
import boto3
import pymysql
import fnmatch

s3 = boto3.client('s3', aws_access_key_id='', aws_secret_access_key='', region_name='ap-southeast-1')
etclient = boto3.client('elastictranscoder',aws_access_key_id='', aws_secret_access_key='', region_name='ap-southeast-1')

preset_fn_hls_400='1489495952065-p5hy8m'
preset_fn_hls_600='1489572131781-p3kdil'
#pipelineid='1487579785951-h829v4'
pipelineid='1489495591120-q1h8h5'

    
def lambda_handlerUpload(event, context):    
    for record in event['Records']: 
        bucket=record['s3']['bucket']['name']
        print "Current s3 bucket--" +bucket
        key=record['s3']['object']['key']
        print "Current Object in this bucket--" +key 
        keyInput=key.split('/')[3]
        print "printing key Input"+(keyInput)
        keyPrefix=keyInput.split('.')[0]
        print "printing key prefix"+(keyPrefix)
        key1=keyPrefix.split('D')[1]

        if fnmatch.fnmatch(key,'*'):
            print "printing key file--"+key
            print "Filename without extension--"+keyPrefix
            outHls400=keyPrefix + "_hls_400k"
            outHls600=keyPrefix + "_hls_600k"
            print "output key hls 400k--"+outHls400
            print "output key hls 600k--"+outHls600
            try:
                db =pymysql.connect(host=host, user=user, passwd=password, db=db)
                sqldim="Select `video_dimension` from `tbl_news_content` where `file_name` ='"+ keyPrefix +"';"
                print "printing sqldim "+sqldim
                cur=db.cursor()
                cur.execute(sqldim)
                cnt=cur.fetchone()     
                key2=cnt[0].split(',')[1]
                print "printing width "+key2
                if int(key2)>480:
                    response = etclient.create_job(
                    PipelineId=pipelineid,
                    Input={
                            'Key': key,
                            'FrameRate': 'auto',
                            'Resolution': 'auto',
                            'AspectRatio': 'auto',
                            'Interlaced': 'auto',
                            'Container': 'auto',      
                    },
                    OutputKeyPrefix='FirstNews/Videos/'+key1+'/',
                    Outputs=[
                           {
                              'Key': outHls400,
                                'Rotate': 'auto',
                                'PresetId': preset_fn_hls_400,
                                'SegmentDuration' : '60',
                                'Watermarks': [
                                    {
                                    
                                    'PresetWatermarkId': 'Channellogo',
                                    'InputKey': 'Logo/watermarklogo240.png',
                                    },
                                    {
                                    
                                    'PresetWatermarkId': 'Overlaytext',
                                    'InputKey': 'Logo/watermarktext240.png',
                                    }                        
                            
                            ],                    
                          },
                          {
                                'Key': outHls600,
                                 'Rotate': 'auto',
                                 'PresetId': preset_fn_hls_600,
                                 'SegmentDuration' : '60',
                                'Watermarks': [
                                    {
                                    
                                    'PresetWatermarkId': 'Channellogo',
                                    'InputKey': 'Logo/watermarklogo480.png',
                                    },
                                    {
                                    
                                    'PresetWatermarkId': 'Overlaytext',
                                    'InputKey': 'Logo/watermarktext480.png',
                                    }                                    
                            
                            ],                     
                           }
                           
                           
                           
                      ],
        
                    Playlists=[
                        {
                            'Name': keyPrefix,
                            'Format': 'HLSv3',
                            'OutputKeys': [
                                    outHls400, outHls600
                            ],
                        }
                     ]    
                    )
                elif int(key2)<480 and int(key2)>=400:
                    response = etclient.create_job(
                    PipelineId=pipelineid,
                    Input={
                            'Key': key,
                            'FrameRate': 'auto',
                            'Resolution': 'auto',
                            'AspectRatio': 'auto',
                            'Interlaced': 'auto',
                            'Container': 'auto',      
                    },
                    OutputKeyPrefix='FirstNews/Videos/'+key1+'/',
                    Outputs=[
                           {
                              'Key': outHls400,
                                'Rotate': 'auto',
                                'PresetId': preset_fn_hls_400,
                                'SegmentDuration' : '60',
                                'Watermarks': [
                                    {
                                    
                                    'PresetWatermarkId': 'Channellogo',
                                    'InputKey': 'Logo/watermarklogo240.png',
                                    },
                                    {
                                    
                                    'PresetWatermarkId': 'Overlaytext',
                                    'InputKey': 'Logo/watermarktext240.png',
                                    }                        
                            
                            ],                    
                          },
                          {
                                'Key': outHls600,
                                 'Rotate': 'auto',
                                 'PresetId': preset_fn_hls_600,
                                 'SegmentDuration' : '60',
                                'Watermarks': [
                                    {
                                    
                                    'PresetWatermarkId': 'Channellogo',
                                    'InputKey': 'Logo/watermarklogo300.png',
                                    },
                                    {
                                    
                                    'PresetWatermarkId': 'Overlaytext',
                                    'InputKey': 'Logo/watermarktext300.png',
                                    }                                    
                            
                            ],                     
                           }
                           
                           
                           
                      ],
        
                    Playlists=[
                        {
                            'Name': keyPrefix,
                            'Format': 'HLSv3',
                            'OutputKeys': [
                                    outHls400, outHls600
                            ],
                        }
                     ]    
                    )
                else:
                    response = etclient.create_job(
                    PipelineId=pipelineid,
                    Input={
                            'Key': key,
                            'FrameRate': 'auto',
                            'Resolution': 'auto',
                            'AspectRatio': 'auto',
                            'Interlaced': 'auto',
                            'Container': 'auto',      
                    },
                    OutputKeyPrefix='FirstNews/Videos/'+key1+'/',
                    Outputs=[
                           {
                              'Key': outHls400,
                                'Rotate': 'auto',
                                'PresetId': preset_fn_hls_400,
                                'SegmentDuration' : '60',
                                'Watermarks': [
                                    {
                                    
                                    'PresetWatermarkId': 'Channellogo',
                                    'InputKey': 'Logo/watermarklogo240.png',
                                    },
                                    {
                                    
                                    'PresetWatermarkId': 'Overlaytext',
                                    'InputKey': 'Logo/watermarktext240.png',
                                    }                        
                            
                            ],                    
                          },
                          {
                                'Key': outHls600,
                                 'Rotate': 'auto',
                                 'PresetId': preset_fn_hls_600,
                                 'SegmentDuration' : '60',
                                'Watermarks': [
                                    {
                                    
                                    'PresetWatermarkId': 'Channellogo',
                                    'InputKey': 'Logo/watermarklogo240.png',
                                    },
                                    {
                                    
                                    'PresetWatermarkId': 'Overlaytext',
                                    'InputKey': 'Logo/watermarktext240.png',
                                    }                                    
                            
                            ],                     
                           }
                           
                           
                           
                      ],
        
                    Playlists=[
                        {
                            'Name': keyPrefix,
                            'Format': 'HLSv3',
                            'OutputKeys': [
                                    outHls400, outHls600
                            ],
                        }
                     ]    
                    )
                    sqlStatus=1
            except Exception, e:
                print str(e)                    
                db.rollback()
                sqlStatus=0
            finally:
                cur.close()
                db.close()
                print "Job respones data---"+str(response)
                jobid=""
                for resp in response['Job']['Id']: 
                    jobid+=resp
                sqlStatus=""
                

            
            try:
                db =pymysql.connect(host=host, user=user, passwd=password, db=db)
                cur = db.cursor()
#                from datetime import datetime
                now = datetime.datetime.now()
#                now = dt.datetime.now()
                var=now.strftime("%Y-%m-%d %H:%M:%S")
                from_zone = tz.gettz('UTC')
                to_zone = tz.gettz('Asia/Kolkata')
#                from datetime import datetime
                utc = datetime.datetime.strptime(var, '%Y-%m-%d %H:%M:%S')
                utc = utc.replace(tzinfo=from_zone)
                print "UTC"+str(utc)
                central = utc.astimezone(to_zone)
                print "Central"+str(central)
                sql="INSERT INTO `tbl_upload_videos` (`videoKey`, `isCompleted`, `jobId`, `Status`,`transcodestartDate`) VALUES ('"+keyPrefix+"', 0, '"+jobid+"','Submitted','"+str(central)+"');"
                print "Inserting into tbl_upload_videos key--"+keyPrefix                
                print "Executing insert sql for tbl_upload+videos--"+sql
                cur.execute(sql)
                db.commit()
                sqlStatus=1
            except Exception, e:
                print str(e)
                db.rollback()
                sqlStatus=0

            finally:
                cur.close()
                db.close()
                
            if(sqlStatus==0):
                try:
                    db =pymysql.connect(host=host, user=user, passwd=password, db=db)
                    cur=db.cursor()
                    now = datetime.datetime.now()
                    var=now.strftime("%Y-%m-%d %H:%M:%S")
                    from_zone = tz.gettz('UTC')
                    to_zone = tz.gettz('Asia/Kolkata')
                    utc = datetime.datetime.strptime(var, '%Y-%m-%d %H:%M:%S')
                    utc = utc.replace(tzinfo=from_zone)
                    print "UTC"+str(utc)
                    central = utc.astimezone(to_zone)
                    print "Central"+str(central)
                    csql="UPDATE `tbl_news_content` SET `upload_status`='0', `modified_date`='"+str(central)+"', `transcode_completed_date`='"+str(central)+"' WHERE `file_name`='"+keyPrefix+"';"
                    print "Executing update statement in tbl_news_content--"+csql
                    cur.execute(csql)
                    db.commit() 
                except Exception, e:
                    print "Excpetion--"+str(e)
                finally:
                    cur.close()
                    db.close()
                
        else:
            print "Current key--"+key