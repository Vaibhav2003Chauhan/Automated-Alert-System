import time
import shutil
import pandas as pd
import os
from telegram import Bot
import requests
from datetime import datetime
from notifypy import Notify


TELEGRAM_BOT_TOKEN ='Your  Bot Token '
CHANNEL_ID = 'unique id of your channel'
LOGS_BASE_DIR = r'C:\Program Files (x86)\Stoxxo\Logs'

bot = Bot(token=TELEGRAM_BOT_TOKEN)
processed_lines = set()
notification = Notify()
# finding out the grid file
print("Finding the log file for processing phase started ")
def find_gridlog_file():
    today_date = datetime.now().strftime('%d-%b-%Y')
    log_folder_path = os.path.join(LOGS_BASE_DIR, today_date)
    gridlog_file_path = os.path.join(log_folder_path, 'GridLog.csv')
    print("Finding file for processing has been completed or stopped ")
    modify_time = os.path.getmtime(gridlog_file_path)
    modify_date = datetime.fromtimestamp(modify_time)
    print('Last Modified on:', modify_date)
    return gridlog_file_path

print("Creating the copy of the file for throwing the errors or warn user ")

def create_copy(gridlog_file_path):
    today_date = datetime.now().strftime('%d-%b-%Y')
    desktop_log_folder = os.path.join(r'C:\Users\Administrator\Desktop\Logs', today_date)
    index_file = os.path.join(r'C:\Users\Administrator\Desktop\Logs', today_date)
    index_file_path = os.path.join(index_file, 'lastindex.txt')
    os.makedirs(desktop_log_folder, exist_ok=True)

    if not os.path.exists(index_file_path):
        print("DIRECTORY CREATED ", today_date)
        with open(index_file_path, 'w') as fp:
            fp.write('0')

    copied_file_path = os.path.join(desktop_log_folder, 'GridLogsTG.csv')

    try:
        shutil.copy(gridlog_file_path, copied_file_path)
        print("File copied successfully.")
    except Exception as e:
        print(f"Error occurred while copying the file: {e}")
    
    return copied_file_path

print("The process of sending messages has been started from here ")

def send_telegram_message(message):
    try:
        base_url='enter the url of your channel in which you want to send request '.format(message)
        requests.get(base_url)
        
        print("An message has been send to the telegram : ")
    except Exception as e:
        print(f"Error sending message to Telegram: {e}")

print("Processing the file means reading the log file and creating its copy for sending out alerts on Telegram ") 

def process_file():
    print("NEW EXECUTION OF PROGRAM START ")
    print('\n')
    print('\n')    
    try:
        # Opening file to fetch out the last executed row index
        today_date = datetime.now().strftime('%d-%b-%Y')
        index_file = os.path.join(r'C:\Users\Administrator\Desktop\Logs', today_date)
        final_indexpath = os.path.join(index_file,'lastindex.txt')
        start=time.time()
        gridlog_file_path = find_gridlog_file()
        end=time.time()
        print("THE Time require to find out the grid file is : ",(end-start))
        # checking whether the file path resides in  the memory or not
        start=time.time()
        if os.path.exists(gridlog_file_path):
            # started the file copying process
            print("CREATING THE COPY NOW ")
            copied_file_path = create_copy(gridlog_file_path)
            print(copied_file_path)
            with open(final_indexpath, 'r') as file:
                lasti_index = float(file.read().strip())
                last_index=int(lasti_index)
                print(final_indexpath)
                print(f"Last Index from File: {last_index}")
        
            # reading the copied file process
            df = pd.read_csv(copied_file_path)
            columns = df.columns.tolist()

            # upper was previously executed data frame which we don't want to execute again
            upper = df.iloc[:int(last_index)]

            # lower was the freshly come data in our log file that we want to execute now
            lower = df.iloc[int(last_index) + 1:]

            # printing both data frame
            print("UPPER DATAFRAME  IS ")
            print(upper)
            print("LOWER DATAFRAME IS  ")
            print(lower)

            # printing the length of Data frame for cross-check with data in the file
            print(f'The Length of df is ', len(df) + 1)
            df=lower
            # Now checking for all the messages has started, and notification will be sent according to this
            if 'Log Type' in columns and 'Message' in columns:
                # migrating through the lower data frame
                for i, (index, row) in enumerate(lower.iterrows(), start=1):
                    shru=time.time()
                    logo=row['Log Type']
                    print(logo)
                    print(f'The Current Row is as : ', index)
                    # fav=df.at[index,'Message']
                    # print(fav)
                    # Checking out attention message and error message in our data frame
                    if logo == 'ATTENTION' or logo=='ERROR' :
                        line = df.at[index, 'Message']                                    
                        print(f"The current dataset been executing is ", i)
                        processed_lines.add(line)
                        print(f"Attention: {line}")

                        # setting notification accordance to trading
                        notification.title = "ATTENTION  or ERROR ALERT"
                        notification.message = "You Got an Attention."
                        notification.audio = r'C:\Users\Administrator\Desktop\Setup\Notification_music\attention.wav'
                        notification.send()
                        # sending attention alert to telegram
                        send_telegram_message(f"ATTENTION : {line}")
                    
                    # Checking out the Warning message in our data frame
                    if logo == 'WARNING':
                        line = df.at[index, 'Message']
                        processed_lines.add(line)
                        print("WARNING")
                        print(f"WARNING: {line}")

                        # setting notification accordance to warning
                        notification.title = "WARNING ALERT"
                        notification.message = "You Got a WARNING."
                        notification.audio = r'C:\Users\Administrator\Desktop\Setup\Notification_music\warning.wav'
                        # sending warning alert to telegram
                        notification.send()
                        send_telegram_message(f"WARNING : {line}")

                    # Checking for TRADING message in this data frame
                    if logo  == 'TRADING':
                        line = df.at[index, 'Message']
                        status_code = line.split(' ')
                        processed_lines.add(line)

                        # Checking message of rejected, cancelled, and 17071 in this loop
                        if 'REJECTED' in status_code or 'CANCELLED' in status_code or '17071' in status_code:
                            print(f"TRADING: {line}")
                            # setting notification accordance to trading
                            notification.title = "TRADING ALERT"
                            notification.message = "You Got a TRADING ALERT."
                            notification.audio = r'C:\Users\Administrator\Desktop\Setup\Notification_music\trading.wav'
                            notification.send()
                            send_telegram_message(f"TRADING : {line}")
                        

                    # updating the file, i.e., the last executed row number
                    with open(final_indexpath, 'w') as file:
                        print("File updation call ")
                        print(f"lastindex is ", last_index)
                        file.write(str(last_index+i))
                    khatam=time.time()
                    print("time require for a single loop creation of {index}",(khatam-shru))
            # throwing statement if log type or message columns are not found in this
            else:
                print("Error: 'Log Type' and/or 'Message' columns not found in the CSV file.")
        else:
            print("GridLog.csv not found in today's log folder.")
        end=time.time()
        print("The time for checking and poping out the notification is :",(end-start))
    except Exception as e:
        # CATCHING THE EXCEPTION IF OS WAS UNABLE TO FIND FILE IN THE SYSTEM
        print(f"An error occurred: {e}")
        
if __name__ == "__main__":
    while True:
        start=time.time()
        process_file()
        end=time.time()
        time.sleep(0.500)
        print("THE TIME REQUIRE FOR FULL  COMPLETION IS : ",(end-start))
        