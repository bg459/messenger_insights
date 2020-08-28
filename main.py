import json
import pandas as pd
import os
import matplotlib.pyplot as plt
from datetime import datetime
from matplotlib.backends.backend_pdf import PdfPages
import numpy as np 
import os

pd.options.mode.chained_assignment = None  # default='warn'

#### PRETTY PLOTS :) #####
def plottop(df):
        df = df.sort_values(['num_messages'], ascending = False)
        top20 = df[:30]
        fig = plt.figure()
        plt.bar(top20['name'], top20['num_messages'])
        plt.xticks(rotation = 90)
        plt.tight_layout()
        plt.title("Most Frequently Messaged")
        return fig

def plotvelocity(df):
        df = df.sort_values(['velocity'], ascending = False)
        top20 = df[:30]
        fig = plt.figure()
        plt.bar(top20['name'], top20['velocity'])
        plt.xticks(rotation = 90)
        plt.tight_layout()
        plt.title("Highest Velocity Chats")
        return fig

def plotlengthdiff(df):
        df = df.sort_values(['name_diff_value'], ascending = False)
        top20 = df[:30]
        fig = plt.figure()
        plt.bar(top20['name_diff'], top20['name_diff_value'])
        plt.xticks(rotation = 90)
        plt.tight_layout()
        plt.title("Highest Difference in Average Chat length")
        return fig

#### HELPER FUNCTIONS #####

# Compute a "message velocity" metric for a df
# Only applicable for more than 20 days of chat

def count_words_in_string(s):
        return len(s.split())

def computelengthdiff(df):
        # Assertion, there are only two senders
        senders = df['sender_name'].unique()
        user1 = df[df['sender_name'] == senders[0]]
        user2 = df[df['sender_name'] == senders[1]]
        user1['content'] = user1['content'].astype(str)
        user1['length'] = user1['content'].apply(count_words_in_string)
        user2['content'] = user2['content'].astype(str)
        user2['length'] = user2['content'].apply(count_words_in_string)
        mean1 = np.mean(user1['length'])
        mean2 = np.mean(user2['length'])
        names = ""
        if mean1 > mean2:
                names = senders[0] + " > " + senders[1]
        else:
                names = senders[1] + " > " + senders[0]
        return names, np.abs(mean1 - mean2)

def compute_velocity(df):
        sum = 0
        dates = df['date'].unique()
        for date in dates:
                temp = df[df['date'] == date]
                sum += len(temp)
        if len(dates) > 20:
                return sum/len(dates)
        return 0

##################################################


## BASIC FUNCTIONS ###



def open_file(file):
        with open(file) as f:
                data = json.load(f)
                participants = list(data['participants'])
                messages = pd.DataFrame.from_dict(data['messages'])
                try:
                        return participants, messages[['sender_name', 'timestamp_ms', 'content']]
                except KeyError:
                        return participants, messages

# This function is run once per user
# Cleans up the Dataframe of each user so it is ready for further analysis. 
def parse_file(file):
        num_messages = 0
        participants = ""
        dfs = []
        # Loop through individual files
        for filename in os.listdir(file):
                if filename.find(".json") >= 0:
                        participants, messages = open_file(file + "/" + filename)
                        participants = list(participants)
                        dfs.append(messages)

        df = pd.concat(dfs)
        if len(df) > 0:
                df['timestamp_ms'] = df['timestamp_ms'].astype(float)
                df['date'] = df['timestamp_ms']/1000
                df['date'] = df['date'].apply(datetime.fromtimestamp)
                df['date'] = df['date'].apply(datetime.date)
                return df
        return 1

##################################################

#### MAIN ####
files = []
names = []
num_messages = []
velocities = []
all_users = []

name_diff = []
name_diff_value = []
for filename in os.listdir(os.getcwd()):
    
    # Loop through Users to find
    if filename.find("_") >= 0 and filename != ".DS_Store":
        cutoff = filename.find("_")
        name = (filename[:cutoff])
        df = parse_file(filename)

        
        if len(df) > 100 and len(df['sender_name'].unique()) == 2:
                names.append(name)
                num_messages.append(len(df))
                # Compute velocity
                velocities.append(compute_velocity(df))

                # Compute difference in message length
                diffname, value = computelengthdiff(df)
                name_diff.append(diffname)
                name_diff_value.append(value)


master = pd.DataFrame({"name": names, "num_messages": num_messages, \
                       "velocity": velocities, "name_diff": name_diff, 'name_diff_value':name_diff_value\
                       })



with PdfPages('foo.pdf') as pdf:
        fig = plottop(master)
        pdf.savefig(fig, bbox_inches='tight')
        fig = plotvelocity(master)
        pdf.savefig(fig, bbox_inches='tight')
        fig = plotlengthdiff(master)
        pdf.savefig(fig, bbox_inches= 'tight')



