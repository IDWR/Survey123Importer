Survey123Import

Dan Narsavage, November 2019

--- Introduction ---
I created this as part of the 2019 WUDR grant to provide a means of importing Survey123 data into our internal database.  My intent 
at the outset was to provide a base that could be extended to import data from Survey123 into any of our in-house databases.
I'm not sure that I accomplished that.


--- Overview ---
This project is composed of one script that is to be executed by a scheduled task, and two python modules which the script uses.  

	--- Script ---
	Survey123DataImport.py.  This must be run under an account with proper permissions in the databases to which it will write.  
	
	--- Module 1: MeasurementDatabaseClient --
	Contains all the logic for querying, updating, and inserting data in the internal measurement database

	--- Module 2: Survey123Client --
	Contains all the logic for querying data from Survey123 feature services


--- Dependencies ---
	Python 3.6+
	arcgis==1.6.1
	dataclasses==0.6
	DateTime==4.3
	pyodbc==4.0.27
	python-dateutil==2.8.0
	pytz==2019.3
	zope.interface==4.6.0


--- IDE ---
This project is set up to use PyCharm.


--- Continuous Integration ---

Jenkins contains a build job at  http://our-internal-host/job/GIS/job/Survey123Imports/job/Survey123Imports/.  This job polls TFS every 5 minutes 
and upon detection of changes inside the Survey123Imports folder will get the latest version and build the wheels for distributing the modules.
If everything goes to plan then you should be able to retrieve everything you need to deploy this solution from Jenkins five minutes after you 
commit to TFS.


--- Deployment ---
NB: If you must install new versions of either of the two packages, you should use "pip install ...".  Note further that pip 
pays attention to version numbers so you should increment the version number of any package you need to upgrade (it its setup.py) 
BEFORE committing to TFS so that the build in Jenkins will use that new verison number to create the wheel files.

1) Install the two wheel files on the machine you wish to run the script
2) Place the 'Survey123DataImport.py' script in an appropriate spot on that computer.
3) Place the 'config.json' in the same folder as the script above.
4) Alter the configuration file above to reflect the environment (dev/test/production) under which you wish to run  
5) Import the 'Survey123ImportScheduledTask.xml' XML file into the machine's task scheduler and alter the following bits
	- Path to the script
	- Windows account under which the task will run (ensure the account has proper permissions to the database(s) being updated)

