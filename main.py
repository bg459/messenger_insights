import json
import pandas as pd
import os
import matplotlib.pyplot as plt
from datetime import datetime
from matplotlib.backends.backend_pdf import PdfPages
import numpy as np 
import os

pd.options.mode.chained_assignment = None  # default='warn'
pd.set_option('display.max_columns', None)


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

def plotvelocity2(df):
        df = df.sort_values(['velocity'], ascending = False)
        top20 = df[:30]
        fig = plt.figure()
        plt.bar(top20['name'], top20['num_days'])
        plt.xticks(rotation = 90)
        plt.tight_layout()
        plt.title("Number of Days Chatted (Highest Velocity)")
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

def plottimediff(df):
        df = df.sort_values(['time_diff'], ascending = False)
        top20 = df[:30]
        fig = plt.figure()
        plt.bar(top20['time_str'], top20['time_diff'])
        plt.xticks(rotation = 90)
        plt.tight_layout()
        plt.title("Longest Response Time")
        plt.suptitle("A > B having value N means A is N times slower to respond, on average")
        return fig

def plotlastmessage(df):
        df = df.sort_values(['time_diff'], ascending = False)
        top20 = df[:30]
        fig = plt.figure()
        plt.bar(top20['time_str'], top20['time_diff'])
        plt.xticks(rotation = 90)
        plt.tight_layout()
        plt.title("Proportion of Last Message of the Day")
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
                return sum/len(dates), len(dates)
        return 0, 0


# I want to compute the time between message blocks. But also need to factor in multiple messages
# If message is like A A A B B A A, I'm interested in how long B takes to respond to A and how long
# A responds to B. Average these all to get some global metric.
def compute_time_between(df):
        users = df['sender_name'].unique()
        times_dict = {users[0]: 0, users[1]: 0}
        count_dict = {users[0]: 0, users[1]: 0}
        df = df.dropna()
        df = df.sort_values(['timestamp_ms'], ascending = False)
        for i in range(len(df)):
                current = df.iloc[i, :]['sender_name']
                if i == len(df) - 1:
                        pass
                else:
                        next_one = df.iloc[i+1, :]['sender_name']
                        try:
                                if next_one != current:
                                        # Compute response time
                                        diff = df.iloc[i, :]['timestamp_ms'] - df.iloc[i+1,:]['timestamp_ms']
                                        if diff < 86400000:
                                                times_dict[current] += diff
                                                count_dict[current] += 1
                        except:
                                pass
                        
        # convert to seconds
        avg_0 = times_dict[users[0]] /(1000 * count_dict[users[0]])
        avg_1 = times_dict[users[1]] /(1000 * count_dict[users[1]])

        result_str = ""
        if avg_0 > avg_1:
                result_str = users[0] + " > " + users[1]
                difference = avg_0 / avg_1
        else:
                result_str = users[1] + " > " + users[0]
                difference = avg_1 / avg_0

        return result_str, difference


# Compute who leaves the last message per day
def compute_last_message(df):
        dates = df['date'].unique()
        users = df['sender_name'].unique()
        user_count = {users[0]:0, users[1]:0}
        for date in dates:
                temp = df[df['date'] == date]
                # Who sent it?
                if len(temp) > 0:
                        sender = temp.iloc[0, :]['sender_name']
                        user_count[sender] += 1
                        
        a = user_count[users[0]]
        b = user_count[users[1]]
        if a < b:
                percent = b/(a+b)
                result_str = users[0] + " > " + users[1]
        else:
                percent = a/(a+b)
                result_str = users[1] + " > " + users[0]
        return result_str, percent
        

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
        
        if len(dfs) > 0:
                df = pd.concat(dfs)
                df['timestamp_ms'] = df['timestamp_ms'].astype(float)
                df['date'] = df['timestamp_ms']/1000
                df['date'] = df['date'].apply(datetime.fromtimestamp)
                df['date'] = df['date'].apply(datetime.date)
                return df
        return pd.DataFrame()

##################################################

#### MAIN ####
files = []
names = []
num_messages = []
velocities = []
all_users = []
num_days = []
name_diff = []
name_diff_value = []

time_str = []
time_diff = []
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
                velocity, num_day = compute_velocity(df)
                velocities.append(velocity)
                num_days.append(num_day)

                # Compute difference in message length
                diffname, value = computelengthdiff(df)
                name_diff.append(diffname)
                name_diff_value.append(value)

        if len(df) > 1000 and len(df['sender_name'].unique()) == 2:
                #a, b = cmpute_time_between(df
                a, b = compute_last_message(df)
                time_str.append(a)
                time_diff.append(b)
                
                


master = pd.DataFrame({"name": names, "num_messages": num_messages, \
                       "velocity": velocities, "name_diff": name_diff, 'name_diff_value':name_diff_value,\
                       "num_days": num_days})

times = pd.DataFrame({"time_str": time_str, "time_diff": time_diff})

with PdfPages('foo.pdf') as pdf:
        fig = plottop(master)
        pdf.savefig(fig, bbox_inches='tight')
        fig = plotvelocity(master)
        pdf.savefig(fig, bbox_inches='tight')
        fig = plotvelocity2(master)
        pdf.savefig(fig, bbox_inches = 'tight')
        fig = plotlengthdiff(master)
        pdf.savefig(fig, bbox_inches= 'tight')
        #fig = plottimediff(times)
        #pdf.savefig(fig, bbox_inches = 'tight')
        fig = plotlastmessage(times)
        pdf.savefig(fig, bbox_inches = 'tight')


