### Project description
Fiery-snap is a _componentized_ scraper/processor that leverages Python micro services to transform data for analysis.  Simply put each little service reads data from a set of 1 or more queues, performs some operation on the data, and then puts it into the outbound queue for additional operations.

### Current implementation details
The current implementation reads Twitter user timelines and attempts to extract
common artifacts (e.g. domains, hashes, URLs, URIs, and potential URLs).  This current implementation has three main components (discussed below) that can be run in a series docker instances, remote services, or all locally.

**TL;DR Installation**

0) Set-up Linux with Docker

1) Clone the repo: ```git clone https://github.com/deeso/fiery-snap.git```

2) Setup Twitter Developer Account and an Email Service Account (I used Google Apps) and update ```sample-prod-config.toml``` accordingly.

```
grep -n CREATE fiery-snap/scripts/sample-prod-config.toml

4:    consumer_key = 'CREATE_TWITTER_DEV_ACCOUNT'
5:    consumer_secret = 'CREATE_TWITTER_DEV_ACCOUNT'
6:    access_token = 'CREATE_TWITTER_DEV_ACCOUNT'
7:    access_token_secret = 'CREATE_TWITTER_DEV_ACCOUNT'  
196:        email = 'CREATE_EMAIL'
199:        password = 'CREATE_PASSWORD'
```

3) Update the IP addresses in ```fiery-snap/config/hosts``` 
```
cat fiery-snap/config/hosts

# this file needs to updated per configuration, 
# usually the IP Address of the host running the
# docker containers
mongodb-host:IP_ADDRESS_OF_MONGO_HOST
redis-queue-host:IP_ADDRESS_OF_REDIS_HOST
top-sites-check:IP_ADDRESS_OF_TOP_SITES_HOST
```

4) Run everything in Docker
```
cd fiery-snap/scripts/
bash initialize_and_run_dockers.sh
```

5) To troubleshoot use Docker logs.  After everything is setup, check to see if evertyhing is running:
```

docker ps --format "{{.Names}}:\t{{.Command}}\t{{.Status}}" | sort

# Spacing added for emphasis

mongo-misc:                    "docker-entrypoint.s…"  Up ...
mongo-sink-processed:          "sh python_cmd.sh"      Up ...
mongo-sink-raw:                "sh python_cmd.sh"      Up ...
redis-misc:                    "docker-entrypoint.s…"  Up ...
top-sites:                     "sh python_cmd.sh"      Up ...
twitter-scraper:               "sh python_cmd.sh"      Up ...
twitter-scraper-email-results: "sh python_cmd.sh"      Up ...
twitter-source:                "sh python_cmd.sh"      Up ...
```

**Requirements** for the docker variant is as follows:
1) Ubuntu (or Unix system with bash installed)
2) Clone of this repository
3) Twitter API keys (see: https://apps.twitter.com/)
4) a working Docker installation

**Requirements** for the local or remote variant is as follows:
1) Ubuntu 16.04 (others might work) with Python 2.7 and ```pip```  installed
2) Clone of this respository
3) Twitter API keys (see: https://apps.twitter.com/)

### Dockerized set-up and installation

#### Ubuntu (AWS) Prep
From a fresh install, just run the following to set things up for Docker. 
```
sudo apt-get update
sudo apt-get upgrade
sudo apt-get install git vim build-essential libssl-dev
sudo apt-get install python-pip virtualenv
sudo -H pip install --upgrade pip
sudo apt-get install     apt-transport-https     ca-certificates     curl     software-properties-common
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
sudo add-apt-repository    "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
   $(lsb_release -cs) \
   stable"
sudo apt-get update
sudo apt-get install docker-ce
sudo gpasswd -a $USER docker
```

#### Docker
Ensure that Docker **is installed**.  Next, edit the ```configs\host``` file.  Specifically, update the IP address with an interface address that is accessible on the machine (or VM hosting) the Dockerized components.  Next, edit the ```master-config.toml``` and update the Twitter API keys.  I personally copy **master-config.toml** to **internal-master-config.toml** so that there is no risk of exposing the keys.  Finally, run ```bash initialize_and_run_dockers.sh master-config.toml```.

### Localized set-up and installation
Ensure that **Python 2.7**, **Python pip**, and **bash** in the Linux environment.  In the main directory, run ```pip install .```.  Next, edit the ```master-config.toml``` and update the Twitter API keys.  I personally copy **master-config.toml** to **internal-master-config.toml** so that there is no risk of exposing the keys.  Finally, change directories into mains and run ```python run-all-multiprocess.py -config master-config.toml```.  Alternatively, each component can be run independently by specifying the given name in the configuration file.  ```python run-all-multiprocess.py -config master-config.toml -name twitter-source```

For installations trying to run the components on independent machines, the IP addresses for the **redis** and **mongo** URIs will need to be updated.  Otherwise, the components may not be able to connect to the correct queues, if at all.

### Reading results from mongo
There is an accompanying script in the ```pull-analytics``` directory.  This simple script demonstrates how to connect to the Mongo Database and perform a query.  If the names for the respective databases and collection found in the original configuration files are unchanged, the script will use those default values to query the database.  Furthermore, a simple Mongo query or the output file may also be specified.  The default output file is ```data.json```. 
The following command to download all the raw and processed tweets and save them to **data.json**: 

```
python read-twitter-data.py -host 10.18.120.11 -processed -raw 
```

This command will query and print summaries for Twitter info found in the last **N** days:
```
read-twitter-data-n-days.py -days 1 -host 10.18.120.11 -processed -content_artifacts -content -all

<snip>
James_inthe_box tweet: @precisionsec gtag is kas74, version is 1000081:  https://t.co/zwNXMpOJmK
James_inthe_box hashes: 
James_inthe_box domains: outdatedbrowser.com, steadfast.net
James_inthe_box urls: https://t.co/zwNXMpOJmK
James_inthe_box ips: 
James_inthe_box emails: 
VK_Intel tweet: @honorary_bot @repnescasb Correct. Just checked - nothing to do with the register trick - there is another techniqu… https://t.co/XNuvYiUseD
VK_Intel hashes: 
VK_Intel domains: vkremez.com
VK_Intel urls: https://t.co/XNuvYiUseD
VK_Intel ips: 
VK_Intel emails: 
</snip>
```

Help command for the ```read-twitter-data.py```:
```
usage: read-twitter-data.py [-h] [-host HOST] [-port PORT] [-processed] [-raw]
                            [-colname COLNAME] [-dbname DBNAME]
                            [-outfile OUTFILE] [-query QUERY]

Read all the documents from the twitter feed document.

optional arguments:
  -h, --help        show this help message and exit
  -host HOST        mongo host
  -port PORT        mongo port
  -processed        read from processed
  -raw              read from raw
  -colname COLNAME  collection name
  -dbname DBNAME    dbname name
  -outfile OUTFILE  file to dump json
  -query QUERY      db query
```

Help command for the ```read-twitter-data-n-days.py```:
```
usage: read-twitter-data-n-days.py [-h] [-host HOST] [-port PORT] [-processed]
                                   [-colname COLNAME] [-dbname DBNAME]
                                   [-outfile OUTFILE] [-query QUERY]
                                   [-days DAYS] [-content]
                                   [-use_twitter_timestamp] [-all] [-hashes]
                                   [-domains] [-emails] [-urls] [-pot_urls]
                                   [-ips] [-content_artifacts]

Read all the documents from the twitter feed document.

optional arguments:
  -h, --help            show this help message and exit
  -host HOST            mongo host
  -port PORT            mongo port
  -processed            read from processed from the default DB and Collection
  -colname COLNAME      collection name
  -dbname DBNAME        dbname name
  -outfile OUTFILE      file to dump json
  -query QUERY          db query
  -days DAYS            number of days to look back (make -1 to disable)
  -content              print tweet before the other stuff
  -use_twitter_timestamp
                        query N days back based on message timestamp instead
                        of ingested date
  -all                  read information from extracted Twitter information
  -hashes               include hashes
  -domains              include domains
  -emails               include emails
  -urls                 include urls
  -pot_urls             include pot_urls
  -ips                  include ips
  -content_artifacts    include content_artifacts
```

### Description of components 
Each component can be configured using a toml file.  These components also run a ```web.py``` application that can be used to manage the service and perform a limited number of functions.  Each page accepts command data in the form of **JSON**.  At a minimum the JSON sent to the service must contain the ```target``` parameter specifying the service name set in the configuration file.

#### twitter-source
**Overview**: This service uses the Twitter API to get user messages and then put the messages in a Kombu queue (see: http://docs.celeryproject.org/projects/kombu/en/latest/).  The messaging format is JSON.  The _primary_ key is ```tm_id``` (Twitter Message ID), which is used as the unique identifier.  This other main _top-level_ keys.

**JSON Message Format**
```
[
 'content':str,              # Text of the message
 'forced_content':bool,      # Whether or not the content had to be coerced
 'link':str,                 # Link to the Twitter message
 'meta':dict,                # Meta data from the Tweet obtained from API
 'obtained_timestamp':str,   # UTC Date retrieved (e.g. "2017-11-01 15:34:35")
 'references':list,          # Potential references
 'source':str,               # Source of the information (e.g. twitter-source)
 'source_type':str,          # Name of the source (e.g. twitter-source)
 'timestamp':str,            # Timestamp of the Tweet 
 'tm_id':str,                # Unique identifier (e.g. Twitter Message ID)
 'user':str                  # Handle (screen name) of the Twitter user
]                 
```

**Service Pages and Commands**

_JsonUploadPage_: Upload messages in the expected **twitter-source JSON** format.  The expected JSON parameters are either: ```msg``` or ```msgs```.  ```msg``` is a single messages, and ```msgs``` is a list of messages. An example URI is: ```http://127.256.256.256:19003/jsonupload/```.  
Example JSON would be: 
```
{
 "target": "twitter-source", 
 "msg": {...}, 
 "msgs":[...]
}
```

_TestPage_: Test to see if the service is running.  An example URI is: ```http://127.256.256.256:19003/testpage/```.  
Example JSON would be: 
```
{"pong":"test", "target": "twitter-source"}
```

_AddHandlesPage_ or _RemoveHandlesPage_: Add or remove Twitter handles to the ingestion without restarting the service.  The expected JSON parameters are either: ```handle``` or ```handles```.  ```handle``` is a single Twitter handle, and ```handles``` is a list of handles. An example URI is: ```http://127.256.256.256:19003/add_handles/``` or ```http://127.256.256.256:19003/rm_handles/```.  
Example JSON would be: 
```
{
 "target": "twitter-source", 
 "handle": "adpridge", 
 "handles":["adpridge", ...]
}
```

_ListHandlesPage_: List the Twitter handles being ingested.  An example URI is: ```http://127.256.256.256:19003/list_handles/```.  
Example JSON would be: 
```
{
 "target": "twitter-source", 
}
```

_ConsumePage_: Trigger a manual consumption operation.  For example, start reading Twitter user's timelines.  An example URI is: ```http://127.256.256.256:19003/consume/```.  
Example JSON would be: 
```
{
 "target": "twitter-source", 
}
```

#### twitter-scraper
**Overview**: This service reads messages retrieved from the ```twitter-source``` and extracts relevant artifacts.  If no artifacts are found, the message is discarded.  The ```tm_id``` is used as the unique identifier.  The keys resulting from this analysis are the following (annotations on **new** fields):
```
[
 'content':str,              # Text of the message
 {'entities': [              # Artifacts found in the Twitter Message
    'domains':list, 
    'emails':list, 
    'hashes':list, 
    'ips':list, 
    'pot_urls':list, 
    'urls:list'
    ]},
 'linked_content':[
    'content_artifacts': {   # Artifacts found in the linked content
        'domains':list, 
        'emails':list, 
        'hashes':list, 
        'ips':list, 
        'links':list, 
    },
    'content_type': str,     # Content type of the data
    'expanded_link':str,     # Expanded link (if HTTP Redirect happens)
    'failed':bool,           # Failed to read content
    'link':str,              # Link to content
    'orig_link':,            # Original link to content
    ],
 'obtained_timestamp',
 'tags',                     # Normalized tags based on configure RegEx
 'timestamp',
 'tm_id',
 'user'
]
```

**Service Pages and Commands**

_JsonUploadPage_: Upload messages in the expected **twitter-scraper JSON** format.  The expected JSON parameters are either: ```msg``` or ```msgs```.  ```msg``` is a single messages, and ```msgs``` is a list of messages. An example URI is: ```http://127.256.256.256:19006/jsonupload/```.  

Example JSON would be: 
```
{
 "target": "twitter-scraper", 
 "msg": {...}, 
 "msgs":[...]
}
```

_TestPage_: Test to see if the service is running.  An example URI is: ```http://127.256.256.256:19006/testpage/```.  

Example JSON would be: 
```
{"pong":"test", "target": "twitter-scraper"}
```

_ConsumePage_: Trigger a manual consumption operation.  For example, read all the subscribed queues and read the expected ```twitter-source JSON``` messages.  An example URI is: ```http://127.256.256.256:19003/consume/```.  

Example JSON would be: 
```
{
 "target": "twitter-scraper", 
}
```


#### mongo-sink
**Overview**: This service stores messages from a queue into a Mongo Database.  The ```_id``` can be specified in the configuration file.  If one is not provided, then the Mongo Database will generate the key.  

**Service Pages and Commands**

_JsonUploadPage_: Upload messages in any JSON format that conforms to Mongo's **key** rules.  The expected JSON parameters are either: ```msg``` or ```msgs```.  ```entry``` is a single messages, and ```entries``` is a list of messages.  Please see other optional parameters below.
An example URI is: ```http://127.256.256.256:19004/jsonupload/```.  

Example JSON would be: 
```
{
 "target": "mongo-sink-raw", 
 "entry": {...}, 
 "entries":[...],
 "update":bool,              # Do not update records if they exist
                             # False no update, True (default) do update
 "direct":bool,              # Insert records directly into the
                             #    DB (bypass the client implementation)
                             #    False (default) use client, 
                             #    True inject directly into the DB 
 "dbname": null or str,      # specify a DB to insert into 
                             #    (requires collection name too)
 "colname": null or str,     # specify a collection to insert into 
                             #    (requires DB name name too)
 ""

}
```

_TestPage_: Test to see if the service is running.  An example URI is: ```http://127.256.256.256:19004/testpage/```.  
Example JSON would be: 
```
{"pong":"test", "target": "mongo-sink-raw"}
```

_ConsumePage_: Trigger a manual consumption operation.  For example, read all the subscribed queues and read messages.  An example URI is: ```http://127.256.256.256:19004/consume/```.  

Example JSON would be: 
```
{
 "target": "mongo-sink-raw", 
}
```




