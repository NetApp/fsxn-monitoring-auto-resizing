# Copyright NetApp 2023. Developed by NetApp Solutions Engineering Team
#
# Description:  This Lambda function can be used to automate the monitoring and autoscaling of
#               FSX ONTAP Storage Capacity based on volume usage. 
# Pre-requisites for running this template
#   - Create lambda in a private subnet with nat gateway for internal access
#   - Update all values in vars.py with the required inputs.
#   - FSX File System and Volumes should be created with FSX API password set.
#   - Ensure that Lambda function has connectivity to FSX by attaching FSxN VPC and subnets to lambda with appropriate security group.
#   - Increase the default timeout for Lambda function from 3 secs. to 5 mins.
#   - Create a new requests Layer and a pramiko layer for this lambda function with python 3.9. (Use the zip files to create the layers)
#   - Create a new trigger with event bridge and select schedule expression and enter rate(1 day) to ensure this function runs once every day.
#   - Allow Lambda to create the default role while creating lambda function and add a new inline policy using "policy.txt" file in this folder. Ensure to replace "${AWS::AccountId}" in policy.txt with your account Id
#   - Ensure that the Sender Email is verified in Amazon SES before using it in this lambda function.
#   - Save the password for fsxadmin in SSM parameter Store and provide the path in fsx_password_ssm_parameter variable in vars.py
#   - Set "warn_notification" variable to True to receive email alerts when a LUN, vol or Storage Capacity crosses 75%.
#   - Set "snapshot_age_threshold_in_days" to the number of days to delete snapshots older than the number of days set 
import json
import requests
requests.packages.urllib3.disable_warnings() 
import base64
import logging
import vars
import math
import time
logger = logging.getLogger()
logger.setLevel(logging.INFO)
import boto3
import botocore
import re
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timezone
def lambda_handler(event, context):
    
    #retrieve fsxn password
    ssm = boto3.client('ssm')
    email_requirements = []
    clone_vol_details = []

    for fsxs in range(len(vars.fsxList)):

        try:
            ssm_response = ssm.get_parameter(Name=vars.fsxList[fsxs]['fsx_password_ssm_parameter'], WithDecryption=True)
            fsxn_password = ssm_response['Parameter']['Value']
        except botocore.exceptions.ClientError as e:
            logger.error(e.response['Error']['Message'])

        vol_details = []
        lun_details = []
        snapshot_details = []
        
        #initialize boto3 fsx
        client_fsx = boto3.client('fsx')
        
        #get fsx storage capacity
        storage_capacity = getStorageCapacity(client_fsx, str(vars.fsxList[fsxs]['fsxId']))
        
        #initialize ontap api auth
        auth_str = str(vars.fsxList[fsxs]['username']) + ":" + str(fsxn_password)
        auth_encoded = auth_str.encode("ascii")
        auth_encoded = base64.b64encode(auth_encoded)
        auth_encoded = auth_encoded.decode("utf-8")
        headers = {
            'authorization': 'Basic {}'.format(auth_encoded),
            'content-type': "application/json",
            'accept': "application/json"
        }
        
        try:
            # URL for fetching aggregate details
            url_aggregate = "https://{}/api/storage/aggregates".format(vars.fsxList[fsxs]['fsxMgmtIp'])

            # Fetch aggregate details
            response_aggregate = requests.get(url_aggregate, headers=headers, verify=False)

            if response_aggregate.status_code == 200:
                # Parse the JSON response
                aggr_data = response_aggregate.json()

                # Extract the list of records
                records = aggr_data.get('records', [])

                # Initialize variables
                aggr_total = None
                aggr_uuid = None

                # Iterate through the records to find the one with name 'aggr1'
                for record in records:
                    if record.get('name') == 'aggr1':
                        aggr_uuid = record.get('uuid')
                        logger.info("UUID found for aggr1: %s", aggr_uuid)
                        break

                if aggr_uuid:
                    # URL for fetching data using UUID
                    url_uuid = "https://{}/api/storage/aggregates/{}".format(vars.fsxList[fsxs]['fsxMgmtIp'], aggr_uuid)

                    # Fetch data using UUID
                    response_uuid = requests.get(url_uuid, headers=headers, verify=False)
                    logger.info("response_uuid: %s", response_uuid)

                    if response_uuid.status_code == 200:
                        # Parse the JSON response
                        uuid_data = response_uuid.json()

                        # Extract relevant fields
                        block_storage = uuid_data.get('space', {}).get('block_storage', {})
                        size_bytes = block_storage.get('size')

                        if size_bytes is not None:
                            logger.info("Block storage details found for aggr1")

                            # Convert bytes to GB
                            size_gb = size_bytes / (1024 ** 3)

                            # Check if the size is in GB or TB
                            if size_gb < 1024:
                                aggr_total = size_gb
                            else:
                                aggr_total = size_gb / 1024

                            logger.info("Aggregate total size: %s GB", aggr_total)
                        else:
                            logger.info("Block storage size not found in UUID output")
                    else:
                        logger.error("Failed to fetch data using UUID: %s %s", response_uuid.status_code, response_uuid.text)
                else:
                    logger.info("UUID not found or aggregate name is not aggr1")
            else:
                logger.error("Failed to fetch aggregate details: %s %s", response_aggregate.status_code, response_aggregate.text)
        except Exception as e:
            logger.error("Error occurred while fetching aggregate details: %s", e)
        #get lun details
        url_lun = "https://{}/api/storage/luns".format(vars.fsxList[fsxs]['fsxMgmtIp'])
        response_lun = requests.get(url_lun, headers=headers, verify=False)

        for i in range(len(response_lun.json()['records'])):
            lun_id = response_lun.json()['records'][i]['uuid']
            url_lun = "https://{}/api/storage/luns/{}".format(vars.fsxList[fsxs]['fsxMgmtIp'],lun_id)
            response_lun_loop = requests.get(url_lun, headers=headers, verify=False)
            lun_details.append(
                {
                    "name": response_lun_loop.json()['location']['logical_unit'], 
                    "vol_name": response_lun_loop.json()['location']['volume']['name'],
                    "vol_uuid": response_lun_loop.json()['location']['volume']['uuid'],
                    "space_total": response_lun_loop.json()['space']['size'],
                    "space_used": response_lun_loop.json()['space']['used'],
                    "space_reserved": response_lun_loop.json()['space']['guarantee']['reserved']
                }
            )
            
            #check if LUN needs resizing and resize if allowed
            lun_per = (float(response_lun_loop.json()['space']['used'])/float(response_lun_loop.json()['space']['size']))*100 
            if(vars.fsxList[fsxs]['warn_notification'] and float(lun_per) > 75 and float(lun_per) < float(vars.fsxList[fsxs]['resize_threshold'])):
                email_requirements.append(
                    {
                        "case": "lun_notification",
                        "name": response_lun_loop.json()['location']['logical_unit'],
                        "use_per": round(lun_per,2),
                        "new_size": 0,
                        "warn": False
                    }
                )


            if(float(lun_per) > float(vars.fsxList[fsxs]['resize_threshold'])):
                new_lun_size = float(response_lun_loop.json()['space']['size']) * 1.05
                new_lun_per = (float(response_lun_loop.json()['space']['used'])/float(new_lun_size))*100 
                while float(new_lun_per) > float(vars.fsxList[fsxs]['resize_threshold']):
                    new_lun_size = new_lun_size * 1.05
                    new_lun_per = (float(response_lun_loop.json()['space']['used'])/float(new_lun_size))*100 
                new_lun_size = math.ceil(new_lun_size)
                
                #check if LUN is thick provisioned
                if(response_lun_loop.json()['space']['guarantee']['reserved'] == True):
                    #check if vol size can accomodate new lun size
                    lun_space_used = 0
                    for lun in lun_details:
                        if(lun['vol_name'] == response_lun_loop.json()['location']['volume']['name']):
                            if(lun['space_reserved'] == True):
                                lun_space_used += lun['space_total']
                            else:
                                lun_space_used += lun['space_used']

                    if(response_lun_loop.json()['space']['guarantee']['reserved'] == True):
                        lun_space_used = lun_space_used - float(response_lun_loop.json()['space']['size']) + new_lun_size
                    else:          
                        lun_space_used = lun_space_used - float(response_lun_loop.json()['space']['used']) + new_lun_size
                    
                    url = "https://{}/api/storage/volumes/{}?fields=*,guarantee".format(vars.fsxList[fsxs]['fsxMgmtIp'],response_lun_loop.json()['location']['volume']['uuid'])
                    response_vol = requests.get(url, headers=headers, verify=False)
                    vol_per = (float(response_vol.json()['space']['size'] - response_vol.json()['space']['available'])/float(response_vol.json()['space']['size']))*100 

                    #update LUN size if vol size can accomodate
                    if(float(lun_space_used * 1.05) < float(response_vol.json()['space']['size'])):
                        try:
                            data = { "space": { "size": new_lun_size}}
                            url_lun_update = "https://{}/api/storage/luns/{}".format(vars.fsxList[fsxs]['fsxMgmtIp'], lun_id)
                            response_lun_update = requests.patch(url_lun_update, headers=headers, json=data, verify=False)
                            if response_lun_update.status_code not in range(200, 300):
                                raise Exception(f"Failed to update LUN size. Status code: {response_lun_update.status_code}, Response: {response_lun_update.text}")
                        except Exception as e:
                            logger.error("An error occurred while updating the LUN size:", e)

                        log = "LUN space used for LUN {} is greater than {}%. LUN resized to: {} GB".format(response_lun_loop.json()['location']['logical_unit'],vars.fsxList[fsxs]['resize_threshold'],round(new_lun_size/(1024*1024*1024),2))
                        logger.info(log)
                        email_requirements.append(
                            {
                                "case": "lun",
                                "name": response_lun_loop.json()['location']['logical_unit'],
                                "use_per": round(lun_per,2),
                                "new_size": new_lun_size,
                                "warn": False
                            }
                        )
                    #update the volume size followed by lun size
                    else:
                        new_vol_size = float(response_vol.json()['space']['size']) * 1.05
                        while(float(lun_space_used) > new_vol_size):
                            new_vol_size *= 1.05
                        new_vol_size_mb = new_vol_size/(1024*1024)
                        new_vol_size_mb = math.ceil(new_vol_size_mb)

                        #Volume is thick provisioned
                        if(response_vol.json()['guarantee']['type'] == "volume"):
                            #check if sc can accomodate new vol size
                            sc_space_used = 0
                            vol_details = getVolDetails(headers, [], vars.fsxList[fsxs]['fsxMgmtIp'])
                            for vol in vol_details:
                                if(vol['guarantee'] == "volume"):
                                    sc_space_used += vol['space_total']
                                else:
                                    sc_space_used += vol['space_used']
                            
                            if(response_vol.json()['guarantee']['type'] == "volume"):
                                sc_space_used = sc_space_used - response_vol.json()['space']['size'] + new_vol_size
                            else:
                                sc_space_used = sc_space_used - response_vol.json()['space']['used'] + new_vol_size
                            
                            sc_space_used = sc_space_used/(1024*1024*1024)

                            #update vol size if sc can accomodate
                            if(float(sc_space_used * 1.1) < float(aggr_total)):
                                #update vol
                                all_vol_details = client_fsx.describe_volumes()
                                for vol in all_vol_details['Volumes']:
                                    if(vol['OntapConfiguration']['UUID'] == response_lun_loop.json()['location']['volume']['uuid']):
                                        vol_id = vol['VolumeId']
                                try:
                                    update = client_fsx.update_volume(VolumeId = vol_id, OntapConfiguration = {'SizeInMegabytes': new_vol_size_mb})
                                except botocore.exceptions.ClientError as e:
                                    logger.error(e.response['Error']['Message'])
                                try:
                                    url_job_monitor = "https://{}/api/cluster/jobs/{}".format(vars.fsxList[fsxs]['fsxMgmtIp'], update['ResponseMetadata']['RequestId'])
                                    job_status = 0
                                    while(job_status not in ["success", "failure"] and int(update['ResponseMetadata']['HTTPStatusCode']) not in range(200,300)):
                                        response_job_monitor = requests.get(url_job_monitor, headers=headers, verify=False)
                                        job_status = response_job_monitor.json()['state']
                                        if job_status == "failure":
                                            logger.info("Failure in updating volume: {}".format(response_job_monitor.json()["error"]["message"]))
                                        if response_job_monitor.status_code not in range(200, 300):
                                            raise Exception(f"Failed to update Volume size. Status code: {response_job_monitor.status_code}, Response: {response_job_monitor.text}")
                                        time.sleep(5)
                                except Exception as e:
                                    logger.error("An error occurred while updating the Volume size:", e)
                                
                                if job_status == "success":
                                    log = "LUN space used for LUN {} is greater than {}%. However volume size for volume {} cannot support increase in LUN size. Hence increasing volume size to {} GB".format(response_lun_loop.json()['location']['logical_unit'],vars.fsxList[fsxs]['resize_threshold'], response_lun_loop.json()['location']['volume']['name'], round((new_vol_size_mb/1024),2))                
                                    logger.info(log)
                                    email_requirements.append(
                                        {
                                            "case": "vol",
                                            "name": response_vol.json()['name'],
                                            "use_per": round(vol_per,2),
                                            "new_size": new_vol_size_mb,
                                            "warn": False
                                        }
                                    )

                                #update lun
                                try:
                                    data = { "space": { "size": new_lun_size}}
                                    url_lun_update = "https://{}/api/storage/luns/{}".format(vars.fsxList[fsxs]['fsxMgmtIp'], lun_id)
                                    response_lun_update = requests.patch(url_lun_update, headers=headers, json=data, verify=False)
                                    if response_lun_update.status_code not in range(200, 300):
                                        raise Exception(f"Failed to update LUN size. Status code: {response_lun_update.status_code}, Response: {response_lun_update.text}")
                                except Exception as e:
                                    logger.error("An error occurred while updating the LUN size:", e)
                                log = "LUN space used for LUN {} is greater than {}%. LUN resized to: {} GB".format(response_lun_loop.json()['location']['logical_unit'],vars.fsxList[fsxs]['resize_threshold'],round(new_lun_size/(1024*1024*1024),2))
                                logger.info(log)
                                email_requirements.append(
                                    {
                                        "case": "lun",
                                        "name": response_lun_loop.json()['location']['logical_unit'],
                                        "use_per": round(lun_per,2),
                                        "new_size": new_lun_size,
                                        "warn": False
                                    }
                                )
                            #else update sc followed by vol followed by lun
                            else:
                                #update sc
                                size = float(storage_capacity) * 1.1
                                while float(size) < float(sc_space_used):
                                    size = size * 1.1
                                size = math.ceil(size)
                                try:
                                    update = client_fsx.update_file_system(FileSystemId = vars.fsxList[fsxs]['fsxId'], StorageCapacity = size)
                                except botocore.exceptions.ClientError as e:
                                    logger.error(e.response['Error']['Message'])
                                log = "Volume {} needs to be resized. However Storage capacity is out of space. Hence, File System Storage Capacity resized to: {} GB".format(response_lun_loop.json()['location']['volume']['name'], size)
                                logger.info(log)
                                email_requirements.append(
                                    {
                                        "case": "sc",
                                        "name": response_lun_loop.json()['location']['volume']['name'],
                                        "use_per": vars.fsxList[fsxs]['resize_threshold'],
                                        "new_size": size,
                                        "warn": True
                                    }
                                )

                                # #update vol
                                # all_vol_details = client_fsx.describe_volumes()
                                # for vol in all_vol_details['Volumes']:
                                #     if(vol['OntapConfiguration']['UUID'] == response_lun_loop.json()['location']['volume']['uuid']):
                                #         vol_id = vol['VolumeId']
                                # update = client_fsx.update_volume(VolumeId = vol_id, OntapConfiguration = {'SizeInMegabytes': new_vol_size_mb})
                                # time.sleep(30)
                                # log = "LUN space used for LUN {} is greater than {}%. However volume size for volume {} cannot support increase in LUN size. Hence increasing volume size to {} GB".format(response_lun_loop.json()['location']['logical_unit'],vars.resize_threshold, response_lun_loop.json()['location']['volume']['name'], round(new_vol_size_mb/1024,2))                
                                # sendEmail("vol", response_vol.json()['name'], vars.resize_threshold, new_vol_size_mb)
                                # logger.info(log)

                                # #update lun
                                # data = { "space": { "size": new_lun_size}}
                                # url_lun_update = "https://{}/api/storage/luns/{}".format(vars.fsxMgmtIp, lun_id)
                                # response_lun_update = requests.patch(url_lun_update, headers=headers, json=data, verify=False)
                                # try:
                                #     response_lun_update.raise_for_status()
                                # except requests.exceptions.HTTPError as e:
                                #     return {
                                #         'statusCode': 400,
                                #         'body': "Error: " + str(e) 
                                #     }
                                # log = "LUN space used for LUN {} is greater than {}%. LUN resized to: {} GB".format(response_lun_loop.json()['location']['logical_unit'],vars.resize_threshold,round(new_lun_size/(1024*1024*1024),2))
                                # sendEmail("lun", response_lun_loop.json()['location']['logical_unit'], vars.resize_threshold, new_lun_size)
                                # logger.info(log)
                        #volume is thin provisioned
                        else:
                            #update vol
                            all_vol_details = client_fsx.describe_volumes()
                            for vol in all_vol_details['Volumes']:
                                if(vol['OntapConfiguration']['UUID'] == response_lun_loop.json()['location']['volume']['uuid']):
                                    vol_id = vol['VolumeId']
                            try:
                                update = client_fsx.update_volume(VolumeId = vol_id, OntapConfiguration = {'SizeInMegabytes': new_vol_size_mb})
                            except botocore.exceptions.ClientError as e:
                                    logger.error(e.response['Error']['Message'])
                            try:
                                url_job_monitor = "https://{}/api/cluster/jobs/{}".format(vars.fsxList[fsxs]['fsxMgmtIp'], update['ResponseMetadata']['RequestId'])
                                job_status = 0
                                while(job_status not in ["success", "failure"] and int(update['ResponseMetadata']['HTTPStatusCode']) not in range(200,300)):
                                    response_job_monitor = requests.get(url_job_monitor, headers=headers, verify=False)
                                    job_status = response_job_monitor.json()['state']
                                    if job_status == "failure":
                                        logger.info("Failure in updating volume: {}".format(response_job_monitor.json()["error"]["message"]))
                                    if response_job_monitor.status_code not in range(200, 300):
                                        raise Exception(f"Failed to update Volume size. Status code: {response_job_monitor.status_code}, Response: {response_job_monitor.text}")
                                    time.sleep(5)
                            except Exception as e:
                                logger.error("An error occurred while updating the Volume size:", e)
                            
                            if job_status == "success":
                                log = "LUN space used for LUN {} is greater than {}%. However volume size for volume {} cannot support increase in LUN size. Hence increasing volume size to {} GB".format(response_lun_loop.json()['location']['logical_unit'],vars.fsxList[fsxs]['resize_threshold'], response_lun_loop.json()['location']['volume']['name'], round(new_vol_size_mb/1024,2))                
                                logger.info(log)
                                email_requirements.append(
                                    {
                                        "case": "vol",
                                        "name": response_vol.json()['name'],
                                        "use_per": round(vol_per,2),
                                        "new_size": new_vol_size_mb,
                                        "warn": False
                                    }
                                )

                            #update lun
                            try:
                                data = { "space": { "size": new_lun_size}}
                                url_lun_update = "https://{}/api/storage/luns/{}".format(vars.fsxList[fsxs]['fsxMgmtIp'], lun_id)
                                response_lun_update = requests.patch(url_lun_update, headers=headers, json=data, verify=False)
                                if response_lun_update.status_code not in range(200, 300):
                                    raise Exception(f"Failed to update LUN size. Status code: {response_lun_update.status_code}, Response: {response_lun_update.text}")
                            except Exception as e:
                                logger.error("An error occurred while updating the LUN size:", e)
                            log = "LUN space used for LUN {} is greater than {}%. LUN resized to: {} GB".format(response_lun_loop.json()['location']['logical_unit'],vars.fsxList[fsxs]['resize_threshold'],round(new_lun_size/(1024*1024*1024),2))
                            logger.info(log)
                            email_requirements.append(
                                {
                                    "case": "lun",
                                    "name": response_lun_loop.json()['location']['logical_unit'],
                                    "use_per": round(lun_per,2),
                                    "new_size": new_lun_size,
                                    "warn": False
                                }
                            )

                #LUN is thin provisioned
                else:
                    #update lun
                    try:
                        data = { "space": { "size": new_lun_size}}
                        url_lun_update = "https://{}/api/storage/luns/{}".format(vars.fsxList[fsxs]['fsxMgmtIp'], lun_id)
                        response_lun_update = requests.patch(url_lun_update, headers=headers, json=data, verify=False)
                        if response_lun_update.status_code not in range(200, 300):
                            raise Exception(f"Failed to update LUN size. Status code: {response_lun_update.status_code}, Response: {response_lun_update.text}")
                    except Exception as e:
                        logger.error("An error occurred while updating the LUN size:", e)
                    log = "LUN space used for LUN {} is greater than {}%. LUN resized to: {} GB".format(response_lun_loop.json()['location']['logical_unit'],vars.fsxList[fsxs]['resize_threshold'],round(new_lun_size/(1024*1024*1024),2))
                    logger.info(log)
                    email_requirements.append(
                        {
                            "case": "lun",
                            "name": response_lun_loop.json()['location']['logical_unit'],
                            "use_per": round(lun_per,2),
                            "new_size": new_lun_size,
                            "warn": False
                        }
                    )
                        

            else:
                log = "LUN space used by LUN {} is less than {}%. LUN Size Used = {}%".format(response_lun_loop.json()['location']['logical_unit'], vars.fsxList[fsxs]['resize_threshold'], round(lun_per,2))
                logger.info(log)

    
    
        #get volume details
        url = "https://{}/api/storage/volumes".format(vars.fsxList[fsxs]['fsxMgmtIp'])
        response = requests.get(url, headers=headers, verify=False)
        
        for i in range(len(response.json()['records'])):
            temp = response.json()['records'][i]['uuid']
            url = "https://{}/api/storage/volumes/{}?fields=*,guarantee,clone.is_flexclone,clone.parent_snapshot.name".format(vars.fsxList[fsxs]['fsxMgmtIp'],temp)
            response_vol = requests.get(url, headers=headers, verify=False)
            if(response_vol.json()["clone"]["is_flexclone"]):
                vol_details.append(
                    {
                        "name": response_vol.json()['name'], 
                        "uuid": response.json()['records'][i]['uuid'],
                        "space_total": response_vol.json()['space']['size'],
                        "space_used": response_vol.json()['space']['size'] - response_vol.json()['space']['available'],
                        "guarantee": response_vol.json()['guarantee']['type'],
                        "is_flexclone": True,
                        "parent_snapshot": response_vol.json()['clone']['parent_snapshot']['name']
                    }
                )
            else:
                vol_details.append(
                    {
                        "name": response_vol.json()['name'], 
                        "uuid": response.json()['records'][i]['uuid'],
                        "space_total": response_vol.json()['space']['size'],
                        "space_used": response_vol.json()['space']['size'] - response_vol.json()['space']['available'],
                        "guarantee": response_vol.json()['guarantee']['type'],
                        "is_flexclone": False,
                        "parent_snapshot": ""
                    }
                )
            
            #check if volume needs resizing and resize if allowed and send email
            vol_per = (float(response_vol.json()['space']['size'] - response_vol.json()['space']['available'])/float(response_vol.json()['space']['size']))*100 
            if(vars.fsxList[fsxs]['warn_notification'] and float(vol_per) > 75 and float(vol_per) < float(vars.fsxList[fsxs]['resize_threshold'])):
                email_requirements.append(
                    {
                        "case": "vol_notification",
                        "name": response_vol.json()['name'],
                        "use_per": round(vol_per,2),
                        "new_size": 0,
                        "warn": False
                    }
                )
            if(float(vol_per) > float(vars.fsxList[fsxs]['resize_threshold'])):
                new_vol_size = float(response_vol.json()['space']['size']) * 1.05
                new_vol_per = (float(response_vol.json()['space']['size'] - response_vol.json()['space']['available'])/float(new_vol_size))*100 
                while float(new_vol_per) > float(vars.fsxList[fsxs]['resize_threshold']):
                    new_vol_size = new_vol_size * 1.05
                    new_vol_per = (float(response_vol.json()['space']['size'] - response_vol.json()['space']['available'])/float(new_vol_size))*100 
                new_vol_size_mb = new_vol_size/(1024*1024)
                new_vol_size_mb = math.ceil(new_vol_size_mb)

                #thick provisioned volume
                if(response_vol.json()['guarantee']['type'] == "volume"):

                    #check if sc can accomodate new vol size
                    sc_space_used = 0
                    vol_details = getVolDetails(headers, [], vars.fsxList[fsxs]['fsxMgmtIp'])
                    for vol in vol_details:
                        if(vol['guarantee'] == "volume"):
                            sc_space_used += vol['space_total']
                        else:
                            sc_space_used += (vol['space_used'])
                    
                    if(response_vol.json()['guarantee']['type'] == "volume"):
                        sc_space_used = sc_space_used - response_vol.json()['space']['size'] + new_vol_size
                    else:
                        sc_space_used = sc_space_used - response_vol.json()['space']['used'] + new_vol_size
                    sc_space_used = sc_space_used/(1024*1024*1024)
                    #update vol size if sc can accomodate
                    if(float(sc_space_used * 1.1) < float(aggr_total)):
                        #update vol
                        all_vol_details = client_fsx.describe_volumes()
                        for vol in all_vol_details['Volumes']:
                            if(vol['OntapConfiguration']['UUID'] == response_vol.json()['uuid']):
                                vol_id = vol['VolumeId']
                        try:
                            update = client_fsx.update_volume(VolumeId = vol_id, OntapConfiguration = {'SizeInMegabytes': new_vol_size_mb})
                        except botocore.exceptions.ClientError as e:
                            logger.error(e.response['Error']['Message'])
                        try:
                            url_job_monitor = "https://{}/api/cluster/jobs/{}".format(vars.fsxList[fsxs]['fsxMgmtIp'], update['ResponseMetadata']['RequestId'])
                            job_status = 0
                            while(job_status not in ["success", "failure"] and int(update['ResponseMetadata']['HTTPStatusCode']) not in range(200,300)):
                                response_job_monitor = requests.get(url_job_monitor, headers=headers, verify=False)
                                job_status = response_job_monitor.json()['state']
                                if job_status == "failure":
                                    logger.info("Failure in updating volume: {}".format(response_job_monitor.json()["error"]["message"]))
                                if response_job_monitor.status_code not in range(200, 300):
                                    raise Exception(f"Failed to update Volume size. Status code: {response_job_monitor.status_code}, Response: {response_job_monitor.text}")
                                time.sleep(5)
                        except Exception as e:
                            logger.error("An error occurred while updating the Volume size:", e)
                        
                        if job_status == "success":
                            log = "Volume space used for volume {} is greater than {}%. Volume resized to: {} GB".format(response_vol.json()['name'], vars.fsxList[fsxs]['resize_threshold'], round(new_vol_size_mb/1024,2))
                            logger.info(log)
                            email_requirements.append(
                                {
                                    "case": "vol",
                                    "name": response_vol.json()['name'],
                                    "use_per": round(vol_per,2),
                                    "new_size": new_vol_size_mb,
                                    "warn": False
                                }
                            )
                    #update sc followed by vol
                    else:
                        #update sc
                        size = float(storage_capacity) * 1.1
                        while float(size) < float(sc_space_used):
                            size = size * 1.1
                        size = math.ceil(size)
                        try:
                            update = client_fsx.update_file_system(FileSystemId = vars.fsxList[fsxs]['fsxId'], StorageCapacity = size)
                        except botocore.exceptions.ClientError as e:
                            logger.error(e.response['Error']['Message'])
                        log = "Volume {} needs to be resized. However Storage capacity is out of space. Hence, File System Storage Capacity resized to: {} GB".format(response_vol.json()['name'], size)
                        logger.info(log)
                        email_requirements.append(
                            {
                                "case": "sc",
                                "name": response_vol.json()['name'],
                                "use_per": vars.fsxList[fsxs]['resize_threshold'],
                                "new_size": size,
                                "warn": True
                            }
                        )

                        #update vol
                        # all_vol_details = client_fsx.describe_volumes()
                        # for vol in all_vol_details['Volumes']:
                        #     if(vol['OntapConfiguration']['UUID'] == response_vol.json()['uuid']):
                        #         vol_id = vol['VolumeId']
                        # update = client_fsx.update_volume(VolumeId = vol_id, OntapConfiguration = {'SizeInMegabytes': new_vol_size_mb})
                        # log = "Volume space used for volume {} is greater than {}%. Volume resized to: {} GB".format(response_vol.json()['name'], vars.resize_threshold, round(new_vol_size_mb/1024,2))
                        # sendEmail("vol", response_vol.json()['name'], vars.resize_threshold, new_vol_size_mb)
                        # logger.info(log)
                #thin provisioned volume
                else:
                    #update vol
                    all_vol_details = client_fsx.describe_volumes()
                    for vol in all_vol_details['Volumes']:
                        if(vol['OntapConfiguration']['UUID'] == response_vol.json()['uuid']):
                            vol_id = vol['VolumeId']
                    try:
                        update = client_fsx.update_volume(VolumeId = vol_id, OntapConfiguration = {'SizeInMegabytes': new_vol_size_mb})
                    except botocore.exceptions.ClientError as e:
                        logger.error(e.response['Error']['Message'])
                    try:
                        url_job_monitor = "https://{}/api/cluster/jobs/{}".format(vars.fsxList[fsxs]['fsxMgmtIp'], update['ResponseMetadata']['RequestId'])
                        job_status = 0
                        while(job_status not in ["success", "failure"] and int(update['ResponseMetadata']['HTTPStatusCode']) not in range(200,300)):
                            response_job_monitor = requests.get(url_job_monitor, headers=headers, verify=False)
                            job_status = response_job_monitor.json()['state']
                            if job_status == "failure":
                                logger.info("Failure in updating volume: {}".format(response_job_monitor.json()["error"]["message"]))
                            if response_job_monitor.status_code not in range(200, 300):
                                raise Exception(f"Failed to update Volume size. Status code: {response_job_monitor.status_code}, Response: {response_job_monitor.text}")
                            time.sleep(5)
                    except Exception as e:
                        logger.error("An error occurred while updating the Volume size:", e)
                    
                    if job_status == "success":
                        log = "Volume space used for volume {} is greater than {}%. Volume resized to: {} GB".format(response_vol.json()['name'], vars.fsxList[fsxs]['resize_threshold'], round(new_vol_size_mb/1024,2))
                        logger.info(log)
                        email_requirements.append(
                            {
                                "case": "vol",
                                "name": response_vol.json()['name'],
                                "use_per": round(vol_per,2),
                                "new_size": new_vol_size_mb,
                                "warn": False
                            }
                        )
                
            else:
                log = "Volume space used by volume {} is less than {}%. Volume Size Used = {}%".format(response_vol.json()['name'], vars.fsxList[fsxs]['resize_threshold'], round(vol_per,2))
                logger.info(log)
        
        
        #calculate % storage capacity used
        total_space_used = 0
        for vol in vol_details:
            if(vol['guarantee'] == "volume"):
                total_space_used += vol['space_total']
            else:
                total_space_used += vol['space_used']
        total_space_used = total_space_used/(1024*1024*1024)
        sc_used_per = (float(total_space_used)/float(aggr_total))*100

        if(vars.fsxList[fsxs]['warn_notification'] and int(sc_used_per * 1.1) > 75 and int(sc_used_per * 1.1) < int(vars.fsxList[fsxs]['resize_threshold'])):
            email_requirements.append(
                {
                    "case": "sc_notification",
                    "name": "null",
                    "use_per": vars.fsxList[fsxs]['resize_threshold'],
                    "new_size": 0,
                    "warn": False
                }
            )
        if int(sc_used_per * 1.1) > int(vars.fsxList[fsxs]['resize_threshold']):
            size = float(aggr_total) * 1.1
            size = float(storage_capacity) + (float(size) - float(aggr_total))
            if(float(size) < 1.1*float(storage_capacity)):
                size = float(storage_capacity) * 1.1
                while float(size) < float(storage_capacity):
                    size *= 1.1 
            size = math.ceil(size)
            try:
                update = client_fsx.update_file_system(FileSystemId = vars.fsxList[fsxs]['fsxId'], StorageCapacity = size)
            except botocore.exceptions.ClientError as e:
                logger.error(e.response['Error']['Message'])
            log = "Total volume space used is greater than {}%. File System Storage Capacity resized to: {} GB".format(vars.fsxList[fsxs]['resize_threshold'],size)
            logger.info(log)
            email_requirements.append(
                {
                    "case": "sc",
                    "name": "null",
                    "use_per": vars.fsxList[fsxs]['resize_threshold'],
                    "new_size": size,
                    "warn": False
                }
            )
        
        else:
            log = "Total volume space used is less than {}%. Storage Capacity = {} GB, Total volume Size Used = {}%".format(vars.fsxList[fsxs]['resize_threshold'], storage_capacity, round(sc_used_per,2))
            logger.info(log)

        
        #Get snapshot details
        if(vars.fsxList[fsxs]['enable_snapshot_deletion']):
            snapshot_details = getSnapshotDetails(headers, vol_details, vars.fsxList[fsxs]['fsxMgmtIp'], snapshot_details)
            for snapshot in snapshot_details:
                snapshot_name_not_present = True
                for volume in vol_details:
                    if volume['parent_snapshot'] == snapshot['name']:
                        snapshot_name_not_present = False
                        break

                try:
                    # Extract the create-time value
                    create_time_str = snapshot["create_time"]
                    create_time = datetime.fromisoformat(create_time_str.replace('Z', '+00:00'))

                    # Calculate how old the snapshot is in days
                    age_days = (datetime.now(timezone.utc) - create_time).days
                    snapshot["age_in_days"] = int(age_days)
                        
                except ValueError as e:
                    logger.error(f"Error parsing create-time value: {create_time_str}")
                    
                try:
                    # Extract the size value from the snapshot details
                    size_bytes = snapshot["size"]
                    snapshot["size_in_bytes"] = size_bytes

                    #delete snapshot if older than threshold
                    if(int(snapshot["age_in_days"]) > vars.fsxList[fsxs]['snapshot_age_threshold_in_days'] and snapshot_name_not_present):
                        url = "https://{}/api/storage/volumes/{}/snapshots/{}".format(vars.fsxList[fsxs]['fsxMgmtIp'], snapshot["vol_uuid"], snapshot["uuid"])
                        try:
                            response_ss_delete = requests.delete(url, headers=headers, verify=False)
                        except Exception as e:
                            logger.error(f"An error occurred while deleting the Snapshot: {e}")
                        try:
                            url_job_monitor = "https://{}/api/cluster/jobs/{}".format(vars.fsxList[fsxs]['fsxMgmtIp'], response_ss_delete.json()['job']['uuid'])
                            job_status = 0
                            while(job_status not in ["success", "failure"]):
                                response_job_monitor = requests.get(url_job_monitor, headers=headers, verify=False)
                                job_status = response_job_monitor.json()['state']
                                if job_status == "failure":
                                    logger.info("Failure in deleting snapshot %s: %s", snapshot['name'], response_job_monitor.json()["error"]["message"])
                                if response_job_monitor.status_code not in range(200, 300):
                                    raise Exception("Failed to delete Snapshot %s. Status code: %d, Response: %s" % (snapshot['name'], response_job_monitor.status_code, response_job_monitor.text))
                                time.sleep(5)
                        except Exception as e:
                            logger.error("An error occurred while deleting the Snapshot %s: %s", snapshot["name"], e)
                        
                        if job_status == "success":
                            log = "Snapshot %s for volume %s has been deleted as it is %d days old which is above the threshold of %d days." % (snapshot['name'], snapshot['vol_name'], int(snapshot['age_in_days']), vars.fsxList[fsxs]['snapshot_age_threshold_in_days'])
                            logger.info(log)
                            email_requirements.append(
                                {
                                    "case": "snapshot_delete",
                                    "name": snapshot,
                                    "use_per": snapshot["vol_name"],
                                    "new_size": int(age_days),
                                    "warn": False
                                }
                            )

                except Exception as e:
                    logger.error("Error while fetching size value: %s", e)
        
        #populate flexclone details
        for vol in vol_details:
            if(vol["is_flexclone"]):
                for snapshot in snapshot_details:
                    if(vol["parent_snapshot"] == snapshot["name"]):
                        clone_vol_details.append(
                            {
                                "name": snapshot["vol_name"],
                                "parent_snapshot": vol["parent_snapshot"],
                                "snapshot_size": float(snapshot["size_in_bytes"])/1024
                            }
                        )
        
    #send consolidated email


    sendEmail(email_requirements, clone_vol_details)

    return {
        'statusCode': 200,
        'body': "success"
    }

def sendEmail(email_requirements, clone_vol_details):
    lun_output_str = []
    vol_output_str = []
    sc_output_str = []
    snapshot_output_str = []
    clone_output_str = []
    output_html = ["<h1>FSx for ONTAP Monitoring</h1><br>"]
    
    
    # Define inline styles
    styles = """
    <style>
        .card {
            border: 1px solid #ccc;
            border-radius: 5px;
            box-shadow: 0 0 5px rgba(0, 0, 0, 0.1);
            margin-bottom: 1rem;
        }
        .card-body {
            padding: 1rem;
        }
        .card-title {
            margin-bottom: 1rem;
        }
        .card-text {
            margin-bottom: 0.5rem;
        }
        table {
            border-collapse: collapse;
            width: 100%;
        }
        th, td {
            padding: 0.5rem;
            text-align: left;
        }
        th {
            background-color: #f2f2f2;
        }
    </style>
    """
    
    output_html.insert(0, styles)
    
    
    for email in email_requirements:
        case = email["case"]
        name = email["name"]
        use_per = email["use_per"]
        new_size = email["new_size"]
        warn = email["warn"]

        if(case == "lun"):
            lun_output_str.append("<tr><td>{}</td><td>{}%</td><td style='color: red;'>{}</td><td>{}GB</td></tr>".format(name, use_per, "Resize", round(new_size/(1024*1024*1024),2)))
        elif(case == "vol"):
            vol_output_str.append("<tr><td>{}</td><td>{}%</td><td style='color: red;'>{}</td><td>{}GB</td></tr>".format(name, use_per, "Resize", round(new_size/1024,2)))
        elif(case == "sc" and warn == False):
            sc_output_str.append("<p class='card-text'>Storage Capacity used is greater than {}%. File System Storage Capacity resized to: {} GB</p>".format(use_per , new_size))
        elif(case == "sc" and warn == True):
            sc_output_str.append("<p class='card-text'>Volume {} needs to be resized. However Storage capacity is out of space. Hence, File System Storage Capacity resized to: {} GB. Please run the automation again to update the volume once storage capacity update is completed successfully.</p>".format(name, new_size))
        elif(case == "lun_notification"):
            lun_output_str.append("<tr><td>{}</td><td>{}%</td><td style='color: orange;'>{}</td><td></td></tr>".format(name, use_per, "Warning"))
        elif(case == "vol_notification"):
            vol_output_str.append("<tr><td>{}</td><td>{}%</td><td style='color: orange;'>{}</td><td></td></tr>".format(name, use_per, "Warning"))
        elif(case == "sc_notification"):
            sc_output_str.append("<p class='card-text'>Storage Capacity used is greater than 75%. File System Storage Capacity will be resized once it crosses {}%</p>".format(use_per))
        elif(case == "snapshot_delete"):
            snapshot_output_str.append("<tr><td>{}</td><td>{}</td><td>{} day</td><td>{}KB</td><td style='color: red;'>{}</td></tr>".format(name["name"], use_per, new_size, int(int(name["size_in_bytes"])/1024), "Deleted"))
    
    if(len(clone_vol_details)):
        #add clone vol details to output string
        clone_output_str.append("<div class='card mb-3'><div class='card-body'><h5 class='card-title'>Clone Information</h5><div class='table-responsive'><table class='table table-striped'><thead><tr><th>Volume Name</th><th>Parent Snapshot</th><th>Snapshot Size</th></tr></thead><tbody>")
        
        for clone in clone_vol_details:
            clone_output_str.append("<tr><td>{}</td><td>{}</td><td>{}</td></tr>".format(clone["name"], clone["parent_snapshot"], str(clone["snapshot_size"]) + "KB"))
            
        clone_output_str.append("</tbody></table></div></div></div>")
    
    if len(sc_output_str):
        output_html.append("<div class='card mb-3'><div class='card-body'><h5 class='card-title'>File System Storage Capacity Notification</h5>")
        output_html += sc_output_str
        output_html.append("</div></div></div>")
    if len(vol_output_str):
        output_html.append("<div class='card mb-3'><div class='card-body'><h5 class='card-title'>Volume Notification</h5><div class='table-responsive'><table class='table table-striped'><thead><tr><th>Volume Name</th><th>Use %</th><th>Notification Type</th><th>Updated Size</th></tr></thead><tbody>")
        output_html += vol_output_str
        output_html.append("</tbody></table></div></div></div>")
    if len(lun_output_str):
        output_html.append("<div class='card mb-3'><div class='card-body'><h5 class='card-title'>LUN Notification</h5><div class='table-responsive'><table class='table table-striped'><thead><tr><th>LUN Name</th><th>Use %</th><th>Notification Type</th><th>Updated Size</th></tr></thead><tbody>")
        output_html += lun_output_str
        output_html.append("</tbody></table></div></div></div>")
    if len(snapshot_output_str):
        output_html.append("<div class='card mb-3'><div class='card-body'><h5 class='card-title'>Snapshot Notification</h5><div class='table-responsive'><table class='table table-striped'><thead><tr><th>Snapshot Name</th><th>Volume Name</th><th>Snapshot Age</th><th>Space Freed Up</th><th>Status</th></tr></thead><tbody>")
        output_html += snapshot_output_str
        output_html.append("</tbody></table></div></div></div>")
    if len(clone_output_str):
        output_html += clone_output_str
        
    output_html = '\n'.join(output_html)
    
    SUBJECT = "FSX for ONTAP Monitoring Notification: AWS Lambda"
    
    if len(clone_vol_details) or len(sc_output_str) or len(vol_output_str) or len(lun_output_str) or len(snapshot_output_str) or len(clone_output_str):
        if vars.internet_access == False:
            
            ssm = boto3.client('ssm')
    
            ssm_response = ssm.get_parameter(Name=vars.smtp_password_ssm_parameter, WithDecryption=True)
            smtp_password = ssm_response['Parameter']['Value']
            
            ssm_response = ssm.get_parameter(Name=vars.smtp_username_ssm_parameter, WithDecryption=True)
            smtp_username = ssm_response['Parameter']['Value']
            
            smtp_host = "email-smtp." + vars.smtp_region + ".amazonaws.com"
            smtp_port = 587
            
            msg = MIMEMultipart('related')
            msg['Subject'] = SUBJECT
            msg['From'] = vars.sender_email
            msg['To'] = vars.recipient_email
        
            # Attach the HTML content to the message
            html_part = MIMEText(output_html, 'html')
            msg.attach(html_part)
            
            try:
                # Connect to the SMTP server using a VPC endpoint
                smtp_conn = smtplib.SMTP(smtp_host, smtp_port)
                smtp_conn.starttls()
                smtp_conn.login(smtp_username, smtp_password)
        
                # Send the message via the SMTP server
                smtp_conn.sendmail(vars.sender_email, vars.recipient_email, msg.as_string())
        
                # Disconnect from the SMTP server
                smtp_conn.quit()
        
                logger.info("Email sent!")
                
            except Exception as e:
                logger.error('Email sending failed: {}'.format(e))
        
        else:
            client = boto3.client('ses')
            try:
                response = client.send_email(
                    Destination={
                        'ToAddresses': [
                            vars.recipient_email,
                        ],
                    },
                    Message={
                        'Body': {
                            'Html': {
                                'Data': output_html,
                            },
                        },
                        'Subject': {
                            'Charset': 'UTF-8',
                            'Data': SUBJECT,
                        },
                    },
                    Source=vars.sender_email,
                )
            except botocore.exceptions.ClientError as e:
                logger.error(e.response['Error']['Message'])
            else:
                logger.info("Email sent!")

def getStorageCapacity(client_fsx, fsxId):
    try:
        response_fsx = client_fsx.describe_file_systems(FileSystemIds=[str(fsxId)])
        storage_capacity = str(response_fsx['FileSystems'][0]['StorageCapacity'])
        if storage_capacity == "":
            return {
                'statusCode': 400,
                'body': "Storage Capacity Not Retrieved"
            }
        else:
            return storage_capacity
    except botocore.exceptions.ClientError as e:
        logger.error("Error Occurred while invoking FSX describe_file_systems: {}".format(e))
    except botocore.exceptions.ParamValidationError as error:
        logger.error("The parameters you provided are incorrect: {}".format(error))

def getVolDetails(headers, vol_details, fsxMgmtIp):
    url = "https://{}/api/storage/volumes".format(fsxMgmtIp)
    response = requests.get(url, headers=headers, verify=False)
    
    for i in range(len(response.json()['records'])):
        temp = response.json()['records'][i]['uuid']
        url = "https://{}/api/storage/volumes/{}?fields=*,guarantee,clone.is_flexclone,clone.parent_snapshot.name".format(fsxMgmtIp,temp)
        response_vol = requests.get(url, headers=headers, verify=False)
        if(response_vol.json()["clone"]["is_flexclone"]):
            vol_details.append(
                {
                    "name": response_vol.json()['name'], 
                    "uuid": response.json()['records'][i]['uuid'],
                    "space_total": response_vol.json()['space']['size'],
                    "space_used": response_vol.json()['space']['size'] - response_vol.json()['space']['available'],
                    "guarantee": response_vol.json()['guarantee']['type'],
                    "is_flexclone": True,
                    "parent_snapshot": response_vol.json()['clone']['parent_snapshot']['name']
                }
            )
        else:
            vol_details.append(
                {
                    "name": response_vol.json()['name'], 
                    "uuid": response.json()['records'][i]['uuid'],
                    "space_total": response_vol.json()['space']['size'],
                    "space_used": response_vol.json()['space']['size'] - response_vol.json()['space']['available'],
                    "guarantee": response_vol.json()['guarantee']['type'],
                    "is_flexclone": False,
                    "parent_snapshot": ""
                }
            )
    return vol_details

def getSnapshotDetails(headers, vol_details, fsxMgmtIp, snapshot_details):
    snapshot_details = []
    for vol in vol_details:
        url = "https://{}/api/storage/volumes/{}/snapshots".format(fsxMgmtIp, vol["uuid"])
        response_snapshots = requests.get(url, headers=headers, verify=False)
        for snapshot in response_snapshots.json()["records"]:
            # Fetch detailed snapshot information
            snapshot_url = "https://{}/api/storage/volumes/{}/snapshots/{}".format(fsxMgmtIp, vol["uuid"], snapshot["uuid"])
            try:
                response_snapshot_detail = requests.get(snapshot_url, headers=headers, verify=False)
                snapshot_info = response_snapshot_detail.json()
                snapshot_details.append(
                    {
                        "name": snapshot["name"],
                        "uuid": snapshot["uuid"],
                        "vol_name": vol["name"],
                        "vol_uuid": vol["uuid"],
                        "create_time": snapshot_info["create_time"],
                        "size": snapshot_info["size"]
                    }
                )
            except Exception as e:
                logger.error("Failed to fetch snapshot details: %s", e)
    return snapshot_details
