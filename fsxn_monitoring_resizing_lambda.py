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
logging.getLogger("paramiko").setLevel(logging.WARNING)
import boto3
import botocore
import paramiko
import re
from datetime import datetime, timezone
def lambda_handler(event, context):
    
    #retrieve fsxn password
    ssm = boto3.client('ssm')
    
    ssm_response = ssm.get_parameter(Name=vars.fsx_password_ssm_parameter, WithDecryption=True)
    fsxn_password = ssm_response['Parameter']['Value']

    vol_details = []
    lun_details = []
    snapshot_details = []
    email_requirements = []
    
    #ssh to fsxn and get aggr1 capacity
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(vars.fsxMgmtIp, username=vars.username, password=fsxn_password)
    command = 'df -A -h'
    stdin, stdout, stderr = ssh_client.exec_command(command)
    aggr_output = stdout.read().decode().strip()
    ssh_client.close()
    
    pattern = r'^aggr1\s+(\S+).*$'
    match = re.search(pattern, aggr_output, re.MULTILINE)
    if match:
        aggr_total = match.group(1)
    else:
        print("aggr1 not found in output")
    
    pattern = r"aggr1\s+(\d+\.?\d*)GB"
    match = re.search(pattern, aggr_output)
    if match:
        aggr_total = float(match.group(1))
    else:
        print("Total not found")

    #initialize boto3 fsx
    client_fsx = boto3.client('fsx')
    
    #get fsx storage capacity
    storage_capacity = getStorageCapacity(client_fsx, str(vars.fsxId))
    
    #initialize ontap api auth
    auth_str = str(vars.username) + ":" + str(fsxn_password)
    auth_encoded = auth_str.encode("ascii")
    auth_encoded = base64.b64encode(auth_encoded)
    auth_encoded = auth_encoded.decode("utf-8")
    headers = {
        'authorization': 'Basic {}'.format(auth_encoded),
        'content-type': "application/json",
        'accept': "application/json"
    }
    
    
    
    #get lun details
    url_lun = "https://{}/api/storage/luns".format(vars.fsxMgmtIp)
    response_lun = requests.get(url_lun, headers=headers, verify=False)

    for i in range(len(response_lun.json()['records'])):
        lun_id = response_lun.json()['records'][i]['uuid']
        url_lun = "https://{}/api/storage/luns/{}".format(vars.fsxMgmtIp,lun_id)
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
        if(vars.warn_notification and float(lun_per) > 75 and float(lun_per) < float(vars.resize_threshold)):
            email_requirements.append(
                {
                    "case": "lun_notification",
                    "name": response_lun_loop.json()['location']['logical_unit'],
                    "threshold": vars.resize_threshold,
                    "new_size": 0,
                    "warn": False
                }
            )


        if(float(lun_per) > float(vars.resize_threshold)):
            new_lun_size = float(response_lun_loop.json()['space']['size']) * 1.05
            new_lun_per = (float(response_lun_loop.json()['space']['used'])/float(new_lun_size))*100 
            while float(new_lun_per) > float(vars.resize_threshold):
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
                
                url = "https://{}/api/storage/volumes/{}?fields=*,guarantee".format(vars.fsxMgmtIp,response_lun_loop.json()['location']['volume']['uuid'])
                response_vol = requests.get(url, headers=headers, verify=False)
                #update LUN size if vol size can accomodate
                if(float(lun_space_used * 1.05) < float(response_vol.json()['space']['size'])):
                    data = { "space": { "size": new_lun_size}}
                    url_lun_update = "https://{}/api/storage/luns/{}".format(vars.fsxMgmtIp, lun_id)
                    response_lun_update = requests.patch(url_lun_update, headers=headers, json=data, verify=False)
                    try:
                        response_lun_update.raise_for_status()
                    except requests.exceptions.HTTPError as e:
                        return {
                            'statusCode': 400,
                            'body': "Error: " + str(e) 
                        }
                    log = "LUN space used for LUN {} is greater than {}%. LUN resized to: {} GB".format(response_lun_loop.json()['location']['logical_unit'],vars.resize_threshold,round(new_lun_size/(1024*1024*1024),2))
                    logger.info(log)
                    email_requirements.append(
                        {
                            "case": "lun",
                            "name": response_lun_loop.json()['location']['logical_unit'],
                            "threshold": vars.resize_threshold,
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
                        vol_details = getVolDetails(headers, [], vars.fsxMgmtIp)
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
                            update = client_fsx.update_volume(VolumeId = vol_id, OntapConfiguration = {'SizeInMegabytes': new_vol_size_mb})
                            time.sleep(30)
                            log = "LUN space used for LUN {} is greater than {}%. However volume size for volume {} cannot support increase in LUN size. Hence increasing volume size to {} GB".format(response_lun_loop.json()['location']['logical_unit'],vars.resize_threshold, response_lun_loop.json()['location']['volume']['name'], round((new_vol_size_mb/1024),2))                
                            logger.info(log)
                            email_requirements.append(
                                {
                                    "case": "vol",
                                    "name": response_vol.json()['name'],
                                    "threshold": vars.resize_threshold,
                                    "new_size": new_vol_size_mb,
                                    "warn": False
                                }
                            )

                            #update lun
                            data = { "space": { "size": new_lun_size}}
                            url_lun_update = "https://{}/api/storage/luns/{}".format(vars.fsxMgmtIp, lun_id)
                            response_lun_update = requests.patch(url_lun_update, headers=headers, json=data, verify=False)
                            try:
                                response_lun_update.raise_for_status()
                            except requests.exceptions.HTTPError as e:
                                return {
                                    'statusCode': 400,
                                    'body': "Error: " + str(e) 
                                }
                            log = "LUN space used for LUN {} is greater than {}%. LUN resized to: {} GB".format(response_lun_loop.json()['location']['logical_unit'],vars.resize_threshold,round(new_lun_size/(1024*1024*1024),2))
                            logger.info(log)
                            email_requirements.append(
                                {
                                    "case": "lun",
                                    "name": response_lun_loop.json()['location']['logical_unit'],
                                    "threshold": vars.resize_threshold,
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
                            update = client_fsx.update_file_system(FileSystemId = vars.fsxId, StorageCapacity = size)
                            time.sleep(30)
                            log = "Volume {} needs to be resized. However Storage capacity is out of space. Hence, File System Storage Capacity resized to: {} GB".format(response_lun_loop.json()['location']['volume']['name'], size)
                            logger.info(log)
                            email_requirements.append(
                                {
                                    "case": "sc",
                                    "name": response_lun_loop.json()['location']['volume']['name'],
                                    "threshold": vars.resize_threshold,
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
                        update = client_fsx.update_volume(VolumeId = vol_id, OntapConfiguration = {'SizeInMegabytes': new_vol_size_mb})
                        time.sleep(30)
                        log = "LUN space used for LUN {} is greater than {}%. However volume size for volume {} cannot support increase in LUN size. Hence increasing volume size to {} GB".format(response_lun_loop.json()['location']['logical_unit'],vars.resize_threshold, response_lun_loop.json()['location']['volume']['name'], round(new_vol_size_mb/1024,2))                
                        logger.info(log)
                        email_requirements.append(
                            {
                                "case": "vol",
                                "name": response_vol.json()['name'],
                                "threshold": vars.resize_threshold,
                                "new_size": new_vol_size_mb,
                                "warn": False
                            }
                        )

                        #update lun
                        data = { "space": { "size": new_lun_size}}
                        url_lun_update = "https://{}/api/storage/luns/{}".format(vars.fsxMgmtIp, lun_id)
                        response_lun_update = requests.patch(url_lun_update, headers=headers, json=data, verify=False)
                        try:
                            response_lun_update.raise_for_status()
                        except requests.exceptions.HTTPError as e:
                            return {
                                'statusCode': 400,
                                'body': "Error: " + str(e) 
                            }
                        log = "LUN space used for LUN {} is greater than {}%. LUN resized to: {} GB".format(response_lun_loop.json()['location']['logical_unit'],vars.resize_threshold,round(new_lun_size/(1024*1024*1024),2))
                        logger.info(log)
                        email_requirements.append(
                            {
                                "case": "lun",
                                "name": response_lun_loop.json()['location']['logical_unit'],
                                "threshold": vars.resize_threshold,
                                "new_size": new_lun_size,
                                "warn": False
                            }
                        )

            #LUN is thin provisioned
            else:
                #update lun
                data = { "space": { "size": new_lun_size}}
                url_lun_update = "https://{}/api/storage/luns/{}".format(vars.fsxMgmtIp, lun_id)
                response_lun_update = requests.patch(url_lun_update, headers=headers, json=data, verify=False)
                try:
                    response_lun_update.raise_for_status()
                except requests.exceptions.HTTPError as e:
                    return {
                        'statusCode': 400,
                        'body': "Error: " + str(e) 
                    }
                log = "LUN space used for LUN {} is greater than {}%. LUN resized to: {} GB".format(response_lun_loop.json()['location']['logical_unit'],vars.resize_threshold,round(new_lun_size/(1024*1024*1024),2))
                logger.info(log)
                email_requirements.append(
                    {
                        "case": "lun",
                        "name": response_lun_loop.json()['location']['logical_unit'],
                        "threshold": vars.resize_threshold,
                        "new_size": new_lun_size,
                        "warn": False
                    }
                )
                    

        else:
            log = "LUN space used by LUN {} is less than {}%. LUN Size Used = {}%".format(response_lun_loop.json()['location']['logical_unit'], vars.resize_threshold, round(lun_per,2))
            logger.info(log)

 
   
    #get volume details
    url = "https://{}/api/storage/volumes".format(vars.fsxMgmtIp)
    response = requests.get(url, headers=headers, verify=False)
    
    for i in range(len(response.json()['records'])):
        temp = response.json()['records'][i]['uuid']
        url = "https://{}/api/storage/volumes/{}?fields=*,guarantee".format(vars.fsxMgmtIp,temp)
        response_vol = requests.get(url, headers=headers, verify=False)
        vol_details.append(
            {
                "name": response_vol.json()['name'], 
                "uuid": response.json()['records'][i]['uuid'],
                "space_total": response_vol.json()['space']['size'],
                "space_used": response_vol.json()['space']['size'] - response_vol.json()['space']['available'],
                "guarantee": response_vol.json()['guarantee']['type']
            }
        )
        
        #check if volume needs resizing and resize if allowed and send email
        vol_per = (float(response_vol.json()['space']['size'] - response_vol.json()['space']['available'])/float(response_vol.json()['space']['size']))*100 
        if(vars.warn_notification and float(vol_per) > 75 and float(vol_per) < float(vars.resize_threshold)):
            email_requirements.append(
                {
                    "case": "vol_notification",
                    "name": response_vol.json()['name'],
                    "threshold": vars.resize_threshold,
                    "new_size": 0,
                    "warn": False
                }
            )
        if(float(vol_per) > float(vars.resize_threshold)):
            new_vol_size = float(response_vol.json()['space']['size']) * 1.05
            new_vol_per = (float(response_vol.json()['space']['size'] - response_vol.json()['space']['available'])/float(new_vol_size))*100 
            while float(new_vol_per) > float(vars.resize_threshold):
                new_vol_size = new_vol_size * 1.05
                new_vol_per = (float(response_vol.json()['space']['size'] - response_vol.json()['space']['available'])/float(new_vol_size))*100 
            new_vol_size_mb = new_vol_size/(1024*1024)
            new_vol_size_mb = math.ceil(new_vol_size_mb)

            #thick provisioned volume
            if(response_vol.json()['guarantee']['type'] == "volume"):

                #check if sc can accomodate new vol size
                sc_space_used = 0
                vol_details = getVolDetails(headers, [], vars.fsxMgmtIp)
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
                    update = client_fsx.update_volume(VolumeId = vol_id, OntapConfiguration = {'SizeInMegabytes': new_vol_size_mb})
                    log = "Volume space used for volume {} is greater than {}%. Volume resized to: {} GB".format(response_vol.json()['name'], vars.resize_threshold, round(new_vol_size_mb/1024,2))
                    logger.info(log)
                    email_requirements.append(
                        {
                            "case": "vol",
                            "name": response_vol.json()['name'],
                            "threshold": vars.resize_threshold,
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
                    update = client_fsx.update_file_system(FileSystemId = vars.fsxId, StorageCapacity = size)
                    time.sleep(30)
                    log = "Volume {} needs to be resized. However Storage capacity is out of space. Hence, File System Storage Capacity resized to: {} GB".format(response_vol.json()['name'], size)
                    logger.info(log)
                    email_requirements.append(
                        {
                            "case": "sc",
                            "name": response_vol.json()['name'],
                            "threshold": vars.resize_threshold,
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
                update = client_fsx.update_volume(VolumeId = vol_id, OntapConfiguration = {'SizeInMegabytes': new_vol_size_mb})
                log = "Volume space used for volume {} is greater than {}%. Volume resized to: {} GB".format(response_vol.json()['name'], vars.resize_threshold, round(new_vol_size_mb/1024,2))
                logger.info(log)
                email_requirements.append(
                    {
                        "case": "vol",
                        "name": response_vol.json()['name'],
                        "threshold": vars.resize_threshold,
                        "new_size": new_vol_size_mb,
                        "warn": False
                    }
                )
            
        else:
            log = "Volume space used by volume {} is less than {}%. Volume Size Used = {}%".format(response_vol.json()['name'], vars.resize_threshold, round(vol_per,2))
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

    if(vars.warn_notification and int(sc_used_per * 1.1) > 75 and int(sc_used_per * 1.1) < int(vars.resize_threshold)):
        email_requirements.append(
            {
                "case": "sc_notification",
                "name": "null",
                "threshold": vars.resize_threshold,
                "new_size": 0,
                "warn": False
            }
        )
    if int(sc_used_per * 1.1) > int(vars.resize_threshold):
        size = float(aggr_total) * 1.1
        while float(size) < float(aggr_total):
            size = size * 1.1
        size = float(storage_capacity) + (float(size) - float(aggr_total))
        if(float(size) < 1.1*float(storage_capacity)):
            while float(size) < float(storage_capacity):
                size = 1.1 * float(storage_capacity)
        size = math.ceil(size)
        update = client_fsx.update_file_system(FileSystemId = vars.fsxId, StorageCapacity = size)
        log = "Total volume space used is greater than {}%. File System Storage Capacity resized to: {} GB".format(vars.resize_threshold,size)
        logger.info(log)
        email_requirements.append(
            {
                "case": "sc",
                "name": "null",
                "threshold": vars.resize_threshold,
                "new_size": size,
                "warn": False
            }
        )
    
    else:
        log = "Total volume space used is less than {}%. Storage Capacity = {} GB, Total volume Size Used = {}%".format(vars.resize_threshold, storage_capacity, round(sc_used_per,2))
        logger.info(log)

    
    #Get snapshot details
    if(vars.enable_snapshot_deletion):
        snapshot_details = getSnapshotDetails(headers, vol_details, vars.fsxMgmtIp, snapshot_details)
        for snapshot in snapshot_details:
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(vars.fsxMgmtIp, username=vars.username, password=fsxn_password)
            command = 'snapshot show -volume ' + snapshot["vol_name"] + ' -snapshot ' + snapshot["name"] + ' -fields create-time'
            stdin, stdout, stderr = ssh_client.exec_command(command)
            ss_create_time_output = stdout.read().decode().strip()
            command = 'snapshot show -volume ' + snapshot["vol_name"] + ' -snapshot ' + snapshot["name"] + ' -fields size'
            stdin, stdout, stderr = ssh_client.exec_command(command)
            ss_size_output = stdout.read().decode().strip()
            ssh_client.close()
            
            # Extract the create-time value using regular expression
            match = re.search(r'\w+\s+\w+\s+\d+\s+\d+:\d+:\d+\s+\d+', ss_create_time_output)
            if match:
                create_time_str = match.group(0)
            else:
                print("Cannot find create-time value in output")
            
            try:
                create_time = datetime.strptime(create_time_str, '%a %b %d %H:%M:%S %Y').replace(tzinfo=timezone.utc)
                
                # Convert the create-time string to datetime object
                create_time = datetime.strptime(create_time_str, '%a %b %d %H:%M:%S %Y').replace(tzinfo=timezone.utc)
                
                # Calculate how old the snapshot is in days
                age_days = (datetime.now(timezone.utc) - create_time).days
                snapshot["age_in_days"] = int(age_days)
                    
            except ValueError as e:
                print(f"Error parsing create-time value: {create_time_str}")
                
            try:
                # Extract the size value from the output
                rows = ss_size_output.split("\n")
                last_row = rows[-1].split()
                size_str = last_row[3]
                
                # Extract the unit from the size string (i.e., the last two characters)
                unit = size_str[-2:].upper()
                
                # Convert the size value to an integer
                size_value = int(size_str[:-2])
                
                # Define a dictionary to map units to conversion factors
                unit_map = {"KB": 1024, "MB": 1024 ** 2, "GB": 1024 ** 3, "TB": 1024 ** 4}
                
                # Check if the unit is valid and convert the size value to bytes
                if unit in unit_map:
                    size_bytes = size_value * unit_map[unit]
                    snapshot["size_in_bytes"] = size_bytes
                else:
                    print("Invalid unit")

                #delete snapshot if older than threshold
                if(int(snapshot["age_in_days"]) > vars.snapshot_age_threshold_in_days):
                    url = "https://{}/api/storage/volumes/{}/snapshots/{}".format(vars.fsxMgmtIp, snapshot["vol_uuid"], snapshot["uuid"])
                    response_ss_delete = requests.delete(url, headers=headers, verify=False)
                    log = "Snapshot {} for volume {} has been deleted as it is {} days old which is above the threshold of {} days. ".format(snapshot["name"], snapshot["vol_name"], int(snapshot["age_in_days"]), vars.snapshot_age_threshold_in_days)
                    logger.info(log)
                    email_requirements.append(
                        {
                            "case": "snapshot_delete",
                            "name": snapshot,
                            "threshold": snapshot["vol_name"],
                            "new_size": int(age_days),
                            "warn": False
                        }
                    )

            except ValueError as e:
                 print(f"Error parsing size value: {size_str}")
                 
    #send consolidated email
    sendEmail(email_requirements)

    return {
        'statusCode': 200,
        'body': "success"
    }

def sendEmail(email_requirements):
    output_str = []
    for email in email_requirements:
        case = email["case"]
        name = email["name"]
        threshold = email["threshold"]
        new_size = email["new_size"]
        warn = email["warn"]

        if(case == "lun"):
            output_str.append("LUN space used for LUN {} is greater than {}%. LUN resized to: {} GB".format(name,threshold,round(new_size/(1024*1024*1024),2)))
        elif(case == "vol"):
            output_str.append("Volume space used for volume {} is greater than {}%. Volume resized to: {} GB".format(name, threshold, round(new_size/1024,2)))
        elif(case == "sc" and warn == False):
            output_str.append("Storage Capacity used is greater than {}%. File System Storage Capacity resized to: {} GB".format(threshold , new_size))
        elif(case == "sc" and warn == True):
            output_str.append("Volume {} needs to be resized. However Storage capacity is out of space. Hence, File System Storage Capacity resized to: {} GB. Please run the automation again to update the volume once storage capacity update is completed successfully.".format(name, new_size))
        elif(case == "lun_notification"):
            output_str.append("LUN space used for LUN {} is greater than 75%. LUN will be resized once it crosses {}%".format(name,threshold))
        elif(case == "vol_notification"):
            output_str.append("Volume space used for volume {} is greater than 75%. Volume will be resized once it crosses {}%".format(name, threshold))
        elif(case == "sc_notification"):
            output_str.append("Storage Capacity used is greater than 75%. File System Storage Capacity will be resized once it crosses {}%".format(threshold))
        elif(case == "snapshot_delete"):
            output_str.append("Snapshot {} for volume {} has been deleted as it is {} days old. Amount of space freed up = {} KB".format(name["name"], threshold, new_size, int(int(name["size_in_bytes"])/1024)))
        
    output_str = '\n'.join(output_str)
    
    SUBJECT = "FSX for NetApp ONTAP Monitoring Notification: AWS Lambda"
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
                    'Text': {
                        'Charset': 'UTF-8',
                        'Data': output_str,
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
        logger.info(e.response['Error']['Message'])
    else:
        logger.info("Email sent!"),

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
        logger.info("Error Occurred while invoking FSX describe_file_systems: {}".format(e))
    except botocore.exceptions.ParamValidationError as error:
        logger.info("The parameters you provided are incorrect: {}".format(error))

def getVolDetails(headers, vol_details, fsxMgmtIp):
    url = "https://{}/api/storage/volumes".format(fsxMgmtIp)
    response = requests.get(url, headers=headers, verify=False)
    
    for i in range(len(response.json()['records'])):
        temp = response.json()['records'][i]['uuid']
        url = "https://{}/api/storage/volumes/{}?fields=*,guarantee".format(fsxMgmtIp,temp)
        response_vol = requests.get(url, headers=headers, verify=False)
        vol_details.append(
            {
                "name": response_vol.json()['name'], 
                "uuid": response.json()['records'][i]['uuid'],
                "space_total": response_vol.json()['space']['size'],
                "space_used": response_vol.json()['space']['size'] - response_vol.json()['space']['available'],
                "guarantee": response_vol.json()['guarantee']['type']
            }
        )
    return vol_details

def getSnapshotDetails(headers, vol_details, fsxMgmtIp, snapshot_details):
    for vol in vol_details:
        url = "https://{}/api/storage/volumes/{}/snapshots".format(fsxMgmtIp, vol["uuid"])
        response_snapshots = requests.get(url, headers=headers, verify=False)
        for snapshot in response_snapshots.json()["records"]:
            snapshot_details.append(
                {
                    "name": snapshot["name"],
                    "uuid": snapshot["uuid"],
                    "vol_name": vol["name"],
                    "vol_uuid": vol["uuid"]
                }    
        )
    return snapshot_details
        