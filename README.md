# fsxn-monitoring-auto-resizing
FSx for ONTAP is a first party enterprise-grade cloud storage service available on AWS that provides highly reliable, scalable, high-performing and feature-rich file storage built on the popular NetApp ONTAP file system. 

FSx for ONTAP provides seamless deployment and management. No storage expertise is required to get started. To further simplify monitoring, an AWS lamdba function (to automate resizing of total storage capacity, volume size or LUN size based on threshold) can be used.  This document provides a step by step guide to create an automated setup that monitors FSx for ONTAP at regular intervals, notifies and resizes when a user-specified threshold is crossed and notifies the administrator of the resizing activity. 

## License
By accessing, downloading, installing or using the content in this repository, you agree the terms of the License laid out in License file.

Note that there are certain restrictions around producing and/or sharing any derivative works with the content in this repository. Please make sure you read the terms of the License before using the content. If you do not agree to all of the terms, do not access, download or use the content in this repository.

Copyright: 2023 NetApp Inc.

## Features
The solution provides the following features:

* Ability to monitor:
  * Usage of overall Storage Capacity of FSx for ONTAP
  * Usage of each volume (thin provisioned / thick provisioned)
  * Usage of each LUN (thin provisioned / thick provisioned)
* Ability to resize any of the above when a user-defined threshold is breached
* Alerting mechanism to receive usage warning and resizing notifications via email
* Ability to delete snapshots older than user-defined threshold
* Ability to get a list of FlexClone volumes and snapshots associated
* Ability to run the checks at a regular interval

## Pre-requisites
Before you begin, ensure that the following prerequisites are met: 

* FSx for ONTAP is deployed
* A Private Subnet with a NAT gateway attached is required by the lambda function for internet connectivity
* The private subnet should also have connectivity to FSx for ONTAP
* "fsxadmin" password has been set for FSx for ONTAP



## Solution Architecture and Deployment Guide
The documentation for the solution architecture used and the deployment guidelines are available at <link here>

## Author Information

- [Dhruv Tyagi](mailto:dhruv.tyagi@netapp.com) - NetApp Solutions Engineering Team
- [Niyaz Mohamed](mailto:niyaz.mohamed@netapp.com) - NetApp Solutions Engineering Team