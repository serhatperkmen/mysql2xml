# mysql2xml
XML formatted backup and restore for Mysql databases with cronjob

## Description

This program allows you to backup and restore your mysql databases. Mysql allows to get xml formatted backup using mysqldump --xml but cannot restore it. This program allows you to do that. Also backup operation can be synchronized via setup_cron. 

## Using 

Firstly release file must be executed to making bash commands reachable from any directory.

### Usage of mysql2xml command
mysql2xml hostname databasename rootpassword xmldestinationpath[optional]    
If xmldestinationpath is not specified, the default xml output path will be in the xml folder.

### Usage of xml2mysql command
xml2mysql hostname username password xmlpath newdbname[optional]      
If newdbname is not specified, new created database name will be same with the xml file.

### Usage of setup_cron

Before execute setup_cron you must change that line.    
echo * * * * * "mysql2xmlcron hostname db_user db_passwd" >> /tmp/crontmp        
According to your needs change the backup time, hostname, db_user and db_passwd. 
