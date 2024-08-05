# EMIS Technical Assessment

# Project description

The EMIS Technical Assessment project aims to demonstrate the ability to design scalable, usable and readable SQL code. The primary objectives include identifying patient by area using their postcodes and deriving a list of eligible patients for a local research study based on specific medical criteria.

## Project Tasks

- **Part 1: Patients by Area using Postcode**
    - Identify the number of patients in each postcode area.
    - Review patient counts by gender to ensure sufficient gender distribution.
- **Part 2: Identifying Eligible Patients for Research Study**
    - Select the 2 most suitable postcode areas based on patient count.

    - Derive a list of patients meeting the following criteria:
        - Current Asthma Diagnosis:
            - Must have a current observation in their medical record with relevant clinical codes from the asthma refset (refsetid 999012891000230104) and **not resolved**.
            - Diagnosis must be current and not resolved.
        - Prescribed Specific Medications:
            - Must have been prescribed any medication containing the following ingredients in the last 30 years:
            - Formoterol Fumarate (codeid 591221000033116, SNOMED concept id 129490002)
            - Salmeterol Xinafoate (codeid 717321000033118, SNOMED concept id 108606009)
            - Vilanterol (codeid 1215621000033114, SNOMED concept id 702408004)
            - Indacaterol (codeid 972021000033115, SNOMED concept id 702801003)
            - Olodaterol (codeid 1223821000033118, SNOMED concept id 704459002)
        - Exclusion Criteria:
            - Patients who are current smokers (have current observation with relevant clinical codes from smoker refset (refsetid 999004211000230104)).
            - Patients who currently weigh less than 40kg (SNOMED concept id 27113001).
            - Patients with a current COPD diagnosis (have current observation in their medical record with relevant clinical codes from COPD refset (refsetid 999011571000230107)).
            - Ensure diagnoses are current and not resolved.
        - Opt-Out Criteria:
            - Only include patients who have not opted out of taking part in research or sharing their medical records (type 1 opt out, connected care opt out).
    
    - Output:
        - The final list of eligible patients should include the following information:
            - Organisation
            - Registration ID
            - Patient ID
            - Full Name
            - Postcode
            - Age
            - Gender


# Installation instructions
From the main/root directory of the project folder, follow these steps. Clone the repository:

1. cd into the directory and then in the command line:
    
bash
    git clone https://github.com/chemi5t/pinterest-data-pipeline905.git

2. Set up a virtual environment for the project:
    
bash
    conda create --name pinterest_env

    
bash
    conda activate pinterest_env

3. Install the required Python packages via the [requirments.txt](requirements.txt) file:
    
bash
    pip install -r requirements.txt

4. Create and set up AWS and Databricks accounts.
5. Save your database credentials to db_creds.yaml for security and to enable data extraction/uploads from/to various sources. 

## Architecture diagram:

This summary outlines the tasks and processes involved in setting up, sending, reading, and processing streaming data using AWS Kinesis and Databricks.

![Architecture digram](Images/28_CloudPinterestPipeline.jpeg)

# Usage instructions

1. Data emulation
    - Milestone 1 and 2

2. Batch processing
    - Milestone 3 to 8

3. Stream Processing
    - Milestone 9

4. Data Querying 
    - [Milestone 7 SQL queries](databricks/_3_pinterest_queries.ipynb)

# Summary of Project Milestones 

## Data Emulation:

- ### **Outcomes from Milestone 1 (Setting up the environment)**

GitHub was successfully set up to allow the project to be saved and tracked for changes via Git and GitHub. VSCode was used for writing the code.

This project uses different services running in the AWS Cloud and thus an AWS Cloud Account is required. 

- ### **Outcomes from Milestone 2 (Creating Pinterest infrastructure via AWS RDS database)**

The Pinterest infrastructure is replicated to resemble the environment of a data engineer at Pinterest. 

A user_posting_emulation_basic.py script is developed, containing RDS database login credentials. The RDS database comprises three tables (pinterest_data, geolocation_data, and user_data) mimicking data obtained from user POST requests to Pinterest:

- pinterest_data: Information about posts updated to Pinterest.
- geolocation_data: Geographic data corresponding to posts in pinterest_data.
- user_data: User information linked to posts in pinterest_data.

A db_creds.yaml file is created to store database credentials securely and excluded from version control using .gitignore.

The script continuously executes, emulating user posting behavior by connecting to the RDS database via SQLAlchemy. It fetches random rows from each table (pin_result, geo_result, and user_result), simulating user activity. The key-value pairing in the dictionaries are noted for later analysis.

python
(base) 
chemi@DELL-laptop MINGW64 ~/AiCore_Projects/pinterest-data-pipeline905 (main)
$ python user_posting_emulation_basic.py 
**************************************************
pin result:  {'index': 7528, 'unique_id': 'fbe53c66-3442-4773-b19e-d3ec6f54dddf', 'title': 'No Title Data Available', 'description': 'No description available Story format', 'poster_name': 'User Info Error', 'follower_count': 'User Info Error', 'tag_list': 'N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e', 'is_image_or_video': 'multi-video(story page format)', 'image_src': 'Image src error.', 'downloaded': 0, 'save_location': 'Local save in /data/mens-fashion', 'category': 'mens-fashion'}

geo result:  {'ind': 7528, 'timestamp': datetime.datetime(2020, 8, 28, 3, 52, 47), 'latitude': -89.9787, 'longitude': -173.293, 'country': 'Albania'}

user result:  {'ind': 7528, 'first_name': 'Abigail', 'last_name': 'Ali', 'age': 20, 'date_joined': datetime.datetime(2015, 10, 24, 11, 23, 51)}
**************************************************


Next, log into the AWS console keeping safe your credentials:

- AWS Account ID: <your_AWSId>
- IAM user name: <your_UserId>
- Password: <your_Password>
- The above are values to be replaced by your own and where ever mentioned herein

When using any of the AWS services, make sure to work in us-east-1 region throughout the project.

## Batch Processing:

- ### **Outcomes from Milestone 3 (Batch processing: Configuring the EC2 Kafka client)**

Install Kafka and the IAM MSK authentication package on the client EC2 machine.
Retrieve and note the IAM role ARN (<your_UserId>-ec2-access-role) for cluster authentication.
Configure Kafka client for IAM authentication. Modify the client.properties file in the Kafka installation directory to enable AWS IAM authentication.
Create Kafka topics by retrieving the Bootstrap servers string and the Plaintext Apache Zookeeper connection string from the MSK Management Console.
Create three topics: 
- <your_UserId>.pin for Pinterest posts data, 
- <your_UserId>.geo for post geolocation data, and 
- <your_UserId>.user for post user data.

For further details follow - [Milestone 3 outline](documentations/milestone_3.md)

- ### **Outcomes from Milestone 4 (Batch Processing: Connect a MSK cluster to a S3 bucket)**

For this project it was not required to create a S3 bucket, an IAM role that allows you to write to this bucket or a VPC Endpoint to S3, as these had already been configured for the AWS account.

In this milestone, MSK Connect is utilised to establish a connection between the MSK cluster and an S3 bucket, enabling automatic data storage for all cluster data.

A custom plugin is created with MSK Connect.
Navigate to the S3 console and locate the bucket associated with your <your_UserId> (user-<your_UserId>-bucket).
Download the Confluent.io Amazon S3 Connector on your EC2 client and transfer it to the identified S3 bucket.
Create a custom plugin named <your_UserId>-plugin in the MSK Connect console.
Create a connector with MSK Connect named <your_UserId>-connector in the MSK Connect console.
Configure the connector with the correct bucket name (user-<your_UserId>-bucket) and ensure the topics.regex field follows the format <your_UserId>.*.
Assign the IAM role used for authentication to the MSK cluster (<your_UserId>-ec2-access-role) in the Access permissions tab.
Upon completing these tasks, data passing through the IAM authenticated cluster will be automatically stored in the designated S3 bucket.

For further details follow - [Milestone 4 outline](documentations/milestone_4.md)

- ### **Outcomes from Milestone 5 (Batch Processing: Configuring an API in API Gateway):**
This milestone focuses on building an API to replicate Pinterest's experimental data pipeline. The API will send data to the MSK cluster, which will then be stored in an S3 bucket using the previously configured connector.

A Kafka REST proxy integration method is built for the API. A resource is created for the API to enable a PROXY integration.
A HTTP ANY method is set up for the resource, ensuring the Endpoint URL reflects the correct PublicDNS of the EC2 machine associated with <your_UserId>.
The API is deployed and the Invoke URL noted for future reference.

The Kafka REST proxy is set up on the EC2 client by installing the Confluent package for the Kafka REST proxy on the EC2 client machine.
The kafka-rest.properties file is configured to allow the REST proxy to perform IAM authentication to the MSK cluster.
The REST proxy on the EC2 client machine is started.
Then send data to the API after modifying the user_posting_emulation_basic.py script to user_posting_emulation_batch.py and send data to the Kafka topics via the API Invoke URL.
Confirm data storage in the S3 bucket, observing the folder organisation created by the connector.

For further details follow - [Milestone 5 outline](documentations/milestone_5.md)

- ### **Outcomes from Milestone 6 (Batch processing: Databricks)**
This milestone focuses on setting up a Databricks account and learning to read data from AWS into Databricks.

Set up your own Databricks account followed by mounting the previously created S3 bucket to Databricks.
Mount the desired S3 bucket to the Databricks account to access the batch data.
The Databricks account has full access to S3, eliminating the need to create a new Access Key and Secret Access Key.
Read data from the Delta table located at dbfs:/user/hive/warehouse/authentication_credentials.
Ensure complete paths to JSON objects when reading from S3 (e.g. topics/<your_UserId>.pin/partition=0/).
Create three DataFrames: 

- df_pin for Pinterest post data
- df_geo for geolocation data
- df_user for user data

This summary outlines the tasks involved in configuring Databricks, mounting an S3 bucket and reading data.

For further details follow - [Milestone 6 outline](databricks/_1_mount_s3_to_databricks.ipynb)

- ### **Outcomes from Milestone 7 (Batch processing: Spark on Databricks)**

Perform data cleaning and computations using Spark on Databricks. Apply this to all 3 PySpark DataFrames:

- df_pin for Pinterest post data
- df_geo for geolocation data
- df_user for user data

For further details follow - [Milestone 7 df cleaning outline](databricks/_2_batch_data_processing_from_mounted_s3.ipynb)

It was also demonstrated how valuable insights could be produced by joining the 3 dataframes via the execution of SQL queries.

For further details follow - [Milestone 7 SQL quereis](databricks/_3_pinterest_queries.ipynb)

- ### **Outcomes from Milestone 8 (Batch processing: AWS MWAA)**

Databricks Workloads are orchestrated on AWS MWAA (Managed Workflows for Apache Airflow).

Create and upload a DAG to a AWS MWAA environment. Access to a MWAA environment Databricks-Airflow-env and to its S3 bucket mwaa-dags-bucket had been provided. Thus, not required to create an API token in Databricks to connect to the AWS account, to set up the MWAA-Databricks connection or to create the requirements.txt file.

An Airflow DAG was created that triggers a Databricks Notebook to be run on a specific schedule. This DAG was uploaded to the dags folder in the mwaa-dags-bucket with the follwoing naming, <your_UserId>_dag.py.

Manually trigger the DAG and verify its successful execution.

For further details follow - [Milestone 8 outline](documentations/milestone_8.md)

## Stream Processing:

- ### **Outcomes from Milestone 9 (Stream Processing: AWS Kinesis)**

Finally for streaming data, data is sent to Kinesis and read into Databricks.

Kinesis Data Streams is used to create three streams for the Pinterest data: 

- streaming-<your_UserId>-pin
- streaming-<your_UserId>-geo  
- streaming-<your_UserId>-user

An API is reconfigured with Kinesis Proxy Integration. This requires the creation of a new script, [user_posting_emulation_streaming.py](user_posting_emulation_streaming.py); that sends data from Pinterest tables to corresponding Kinesis streams via API requests.

Databricks ([_4_streaming_data_processing](<databricks/_4_streaming_data_processing (2).ipynb>)) then cleans the ingests streamed data from Kenisis and writes it into delta tables.

For further details follow - [Milestone 9 outline](documentations/milestone_9.md)

# Project file structure

txt
PINTEREST-DATA-PIPELINE905/
├── Images      [used in README.md]
├── LICENSE.md
├── README.md
├── databricks      [Notebooks used in Databricks]
│   ├── _1_mount_s3_to_databricks.ipynb
│   ├── _2_batch_data_processing_from_mounted_s3.ipynb
│   ├── _3_pinterest_queries.ipynb
│   └── _4_streaming_data_processing.ipynb
├── db_creds.yaml   [Private credentials]
├── documentations      [Links to README.md]
│   ├── milestone_3.md
│   ├── milestone_4.md
│   ├── milestone_5.md
│   ├── milestone_8.md
│   └── milestone_9.md
├── file_structure.txt      [Overview of project directory structure]
├── requirements.txt        [Project environment requirements]
├── user_posting_emulation_basic.py
├── user_posting_emulation_batch.py
├── user_posting_emulation_streaming.py
└── your_UserId_dag.py      [DAG file uploaded to AWS MWAA (Airflow UI) to run the Databricks notebook]


- /PINTEREST-DATA-PIPELINE905
    - This folder has all the folders seen above as well as containing .env and .gitignore
    - The .env file points to the stored private credentials
    - *.yaml, *.env, and  __pycache__/ have been added to .gitignore
    - Environment details saved to requirements.txt
    - README.md will cover all aspects of how the project was conducted over 9 milestones

Screen shot of EXPLORER from VS Code containing the above contents:

![Directory](Images/29_directory.jpg)

# Languages

- Python
- PySpark SQL
- Spark SQL

# License information

This project is licensed under the terms of the [MIT License](LICENSE.md). Please see the [LICENSE.md](LICENSE.md) file for details.


