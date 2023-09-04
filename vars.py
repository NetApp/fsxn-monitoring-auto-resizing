fsxList = [
    {
        "fsxMgmtIp": "",
        "fsxId" : "",
        "username" : "",
        "resize_threshold" : 90,
        "fsx_password_ssm_parameter" : "",
        "warn_notification" : True,
        "enable_snapshot_deletion" : True,
        "snapshot_age_threshold_in_days" : 30
    }
]
sender_email = ""
recipient_email = ""
internet_access = False
# if internet access = False, set the below parameters
smtp_region = ""
smtp_username_ssm_parameter = ""
smtp_password_ssm_parameter = ""