from dateutil import tz
import datetime
import dateutil
import boto3
import pymysql
import fnmatch

# This is a lambda function configured to be triggered when an object is created inside the 
# AWS S3 bucket. The function takes in parameters from the database related to the uploaded object
# and then starts a job inside th elastic transcoder

s3 = boto3.client('s3', aws_access_key_id='', aws_secret_access_key='', region_name='ap-southeast-1')
etclient = boto3.client('elastictranscoder',aws_access_key_id='', aws_secret_access_key='', region_name='ap-southeast-1')

preset_fn_hls_400=''
preset_fn_hls_600=''
pipelineid=''

    
def lambda_handlerUpload(event, context):    
    for record in event['Records']: 
        bucket=record['s3']['bucket']['name']
        key=record['s3']['object']['key']
        keyInput=key.split('/')[3]
        keyPrefix=keyInput.split('.')[0]
        key1=keyPrefix.split('D')[1]

        if fnmatch.fnmatch(key,'*'):
            outHls400=keyPrefix + "_hls_400k"
            outHls600=keyPrefix + "_hls_600k"
            try:
                db =pymysql.connect(host=host, user=user, passwd=password, db=db)
                sqldim="Select `video_dimension` from `tbl_news_content` where `file_name` ='"+ keyPrefix +"';"
                cur=db.cursor()
                cur.execute(sqldim)
                cnt=cur.fetchone()     
                key2=cnt[0].split(',')[1]

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
                db.rollback()
                sqlStatus=0
            finally:
                cur.close()
                db.close()
                jobid=""
                for resp in response['Job']['Id']: 
                    jobid+=resp
                sqlStatus=""
                

            
            try:
                db =pymysql.connect(host=host, user=user, passwd=password, db=db)
                cur = db.cursor()
                now = datetime.datetime.now()
                var=now.strftime("%Y-%m-%d %H:%M:%S")
                from_zone = tz.gettz('UTC')
                to_zone = tz.gettz('Asia/Kolkata')
                utc = datetime.datetime.strptime(var, '%Y-%m-%d %H:%M:%S')
                utc = utc.replace(tzinfo=from_zone)
                central = utc.astimezone(to_zone)
                sql="INSERT INTO `tbl_upload_videos` (`videoKey`, `isCompleted`, `jobId`, `Status`,`transcodestartDate`) VALUES ('"+keyPrefix+"', 0, '"+jobid+"','Submitted','"+str(central)+"');"
                cur.execute(sql)
                db.commit()
                sqlStatus=1

            except Exception, e:
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
                    central = utc.astimezone(to_zone)
                    csql="UPDATE `tbl_news_content` SET `upload_status`='0', `modified_date`='"+str(central)+"', `transcode_completed_date`='"+str(central)+"' WHERE `file_name`='"+keyPrefix+"';"
                    cur.execute(csql)
                    db.commit() 
                except Exception, e:
                    print "Exception--"+str(e)
                finally:
                    cur.close()
                    db.close()
                
        else:
            print "Current key--"+key