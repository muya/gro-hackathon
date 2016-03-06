# gro-hackathon
Project for Gro Hackathon held in Nairobi, 5th Mar 2016

"This product uses the NASS API (http://quickstats.nass.usda.gov/api/) but is not endorsed or certified by NASS."

# Setup

### Required OS installations:

Make sure the following system packages are required in order to install this app

* postgresql
* python-psycopg2
* libpq-dev
* python-dev

### Application Setup
You may run the application in a virtualenv.

1. Clone the application to a directory on your computer
2. Enter the directory: `cd gro-hackathon`
3. Create a virtualenv (make sure you name it env): `virtualenv env`
4. Activate the virtualenv: `source env/bin/activate`
5. Install the requirements from the `requirements.txt` file: `pip install -r requirements.txt`
6. Run the application! `python harvest.py --database_host='dbhost'  --database_name='dbname' --database_user='dbuser' --database_pass='dbpassword' --start_date='start date' --end_date='end_date' --api_key='api key from NASS'`
