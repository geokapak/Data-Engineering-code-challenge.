###########################################################################
############################Test Project###################################
#####This is a python code cleaning, transform and manipulating ###########
######## 3 different datasets, after stakeholder's requirements############
###################and load data into database#############################



# Import packages
import pandas as pd
import numpy as np
import mysql.connector
#!pip install mysql-connector-python
import csv
from datetime import datetime, timedelta


# Import data 
deal_df = pd.read_csv(os.path.join('.','data', 'deal_sample.csv'))
deal_activities_df = pd.read_csv(os.path.join('.','data', "deal_activities_sample.csv"))
deal_updates_df = pd.read_csv(os.path.join('.','data', "deal_updates_sample.csv"))


# Transform


###########################################################################
#########################Check data types##################################
######################Uncomment lines 20-26################################
# loop through columns and check data types
# for col in deal_df.columns:
#     print(f"Data type of column '{col}' is {deal_df[col].dtype}")
# for col in deal_activities_df.columns:
#     print(f"Data type of column '{col}' is {deal_activities_df[col].dtype}")
# for col in deal_updates_df.columns:
#     print(f"Data type of column '{col}' is {deal_updates_df[col].dtype}")


###########################################################################
###############Check for Nan values in every df############################
######################Uncomment lines 31-43################################
# loop through columns and check for NaN's for deal_sample
# for col in deal_df.columns:
#     print(f"Number of NaN's in column '{col}' is {deal_df[col].isna().sum()}")
#     ##No NaN in deal_sample

# for col in deal_activities_df.columns:
#     print(f"Number of NaN's in column '{col}' is {deal_activities_df[col].isna().sum()}")
#     #Number of NaN's in column 'marked_as_done_ts' is 1760
# for col in deal_updates_df.columns:
#     print(f"Number of NaN's in column '{col}' is {deal_updates_df[col].isna().sum()}")
    # Number of NaN's in column 'old_value' is 14
    # Number of NaN's in column 'new_value' is 11 


###########################################################################
#########################Clean the data####################################

#Drop rows with NaN values on "Marked_as_done_ts" as I only need the completed ones
deal_activities_df = deal_activities_df.dropna()

#Drop rows with NaN values since there are only few, there is no need to 
# replace them or cluster them because they are either new companies with no old
#values or old who just stoped
deal_updates_df = deal_updates_df.dropna()

# drop all rows with "deleted" equal to True
deal_activities_df = deal_activities_df[deal_activities_df['deleted'] != True]

###########################################################################
#########################Transformation####################################

#######################1rst transformation#################################

#Groupby deal_id and the number of apearance in "deleted"
grouped = deal_activities_df.groupby('deal_id')['deleted'].apply(lambda x: (x == False).sum())

#create a new column with the number of the apearences
deal_activities_df['num_false'] = deal_activities_df['deal_id'].map(grouped)

#New dataframe with the new column and only the id's that we need to compare with
cleaned_deal_activity_df=deal_activities_df

# export dataframe to a new CSV file
cleaned_deal_activity_df.to_csv('new_deal_activities_sample.csv', index=False)

#####################2nd transformation: Average updates##################

# select only the rows where the update_type is "stages", "values", or "status"
valid_updates = ['stage_id', 'value', 'status']
valid_rows = deal_updates_df[deal_updates_df['update_type'].isin(valid_updates)]

# count the number of times each deal_id appears
counts = valid_rows['deal_id'].value_counts()

#count the number of valid rows and unique deal_id
num_valid_rows = valid_rows.count()[0]
num_unique_ids = valid_rows['deal_id'].nunique()

# Get the average amount of updates made to each deal
Average_upd = num_valid_rows/num_unique_ids

print(f"The average amount of undates made to each deal is: {Average_upd:.2f}")

###Deals that have neither activities nor updates should be marked as inactive.

#Create a list with unique values in id column in deal_df
unique_deal_ids = deal_df["id"].unique()

# Get a list of deal ids that do not exist in the deal_updates_df 
missing_ids = [id for id in unique_deal_ids if id not in deal_updates_df["deal_id"].unique()]

# Find the missing ids that have 0 in total_activities
missing_ids_with_zero_activities = list(deal_df.loc[(deal_df["id"].isin(missing_ids)) & (deal_df["Total_activities"] == 0), "id"])

# # Create a new column to save the "inactive" ids in deal_df
deal_df["Active_status"] = np.where(deal_df["id"].isin(missing_ids_with_zero_activities), "inactive", "")
deal_df.to_csv('new_deal_sample.csv', index=False)


#####################3rd transformation: Average updates##################


#keep only the IDs which have the word "won" in the "Status" column
won_deals = deal_df[deal_df['Status'].str.contains('won', case=False)]['id']
won_deals_activities_df = deal_activities_df[deal_activities_df['deal_id'].isin(won_deals)]

#Create a list with only call and email
activity_types = ['call', 'email']

#count how many times there is call and email in column "Type"
activity_counts = won_deals_activities_df['Type'].value_counts().reindex(activity_types, fill_value=0)
total_count = activity_counts.sum()
activity_counts
#Note that call=2944, email=1729
print(f"Total number of call and email activities: {total_count}")

#Store the updates on deal_updates to a new csv
deal_updates_df.to_csv("new_deal_updates.csv", index=False)

###############################################################################
###########################  Optional request #################################

# Convert the "marked_as_done_ts" column to a pandas datetime 
deal_activities_df["marked_as_done_ts"] = pd.to_datetime(deal_activities_df["marked_as_done_ts"], format="%Y-%m-%d")

# Filter the DataFrame to only include rows where the "marked_as_done_ts" column is within the last two years
two_years_ago = datetime.today() - timedelta(days=365*2)
recent_deals_df = deal_activities_df[deal_activities_df["marked_as_done_ts"] >= pd.Timestamp(two_years_ago)]

# Extract the unique "deal_id" values from the filtered DataFrame
recent_deal_ids = recent_deals_df["deal_id"].unique()

# Convert the resulting pandas Series of "deal_id" values to a list
recent_deal_ids_list = list(recent_deal_ids)

# Create a list of all unique deal IDs, including both inactive and recent deals
all_deal_ids = set(missing_ids_with_zero_activities + recent_deal_ids_list)

# Create a dictionary to store the status of each deal ID
deal_status = {}

# Assign the status "inactive" to all IDs in the "missing_ids_with_zero_activities" list
for deal_id in missing_ids_with_zero_activities:
    deal_status[deal_id] = "inactive"

# Assign the status "recent" to all IDs in the "recent_deal_ids_list" that were not already assigned a status
for deal_id in recent_deal_ids_list:
    if deal_id not in deal_status:
        deal_status[deal_id] = "recent"

# Create a new DataFrame from the dictionary of deal statuses
deal_status_df = pd.DataFrame({"deal_id": list(deal_status.keys()), "status": list(deal_status.values())})


#%% 
###############################################################################
###########################  Working for SQL ##################################

# Connect python with local database
cnx = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",# type your password
    database="project_test"
)

cursor = cnx.cursor()

# Load the csv to database tables
filename_deal_sample = "new_deal_sample.csv"
with open(filename_deal_sample, 'r') as csvfile:
    csv_reader = csv.reader(csvfile, delimiter=',')
    next(csv_reader)  # skip the header row
    for row in csv_reader:
        sql = "INSERT INTO new_deal (id, pipeline_id, user_id, status_, value_, Currency, total_activities, active_status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        val = (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7])
        cursor.execute(sql, val)
filename_activities = "new_deal_activities_sample.csv"
with open(filename_activities, 'r') as csvfile:
    csv_reader = csv.reader(csvfile, delimiter=',')
    next(csv_reader)  # skip the header row
    for row in csv_reader:
        sql = "INSERT INTO new_activities (activity_id, deal_id, Type, marked_as_done_ts, deleted, num_false) VALUES (%s, %s, %s, %s, %s, %s)"
        val = (row[0], row[1], row[2], row[3], row[4], row[5])
        cursor.execute(sql, val)

filename_updates = "new_deal_updates.csv"
with open(filename_updates, 'r') as csvfile:
    csv_reader = csv.reader(csvfile, delimiter=',')
    next(csv_reader)  # skip the header row
    for row in csv_reader:
        sql = "INSERT INTO new_deals_updates (deal_id, update_type, old_value, new_value) VALUES (%s, %s, %s, %s)"
        val = (row[0], row[1], row[2], row[3])
        cursor.execute(sql, val)
cnx.commit()

cursor.close()
cnx.close()