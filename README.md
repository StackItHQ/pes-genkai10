[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/AHFn7Vbn)
# Superjoin Hiring Assignment

### Welcome to Superjoin's hiring assignment! 🚀

### Objective
Build a solution that enables real-time synchronization of data between a Google Sheet and a specified database (e.g., MySQL, PostgreSQL). The solution should detect changes in the Google Sheet and update the database accordingly, and vice versa.

### Problem Statement
Many businesses use Google Sheets for collaborative data management and databases for more robust and scalable data storage. However, keeping the data synchronised between Google Sheets and databases is often a manual and error-prone process. Your task is to develop a solution that automates this synchronisation, ensuring that changes in one are reflected in the other in real-time.

### Requirements:
1. Real-time Synchronisation
  - Implement a system that detects changes in Google Sheets and updates the database accordingly.
   - Similarly, detect changes in the database and update the Google Sheet.
  2.	CRUD Operations
   - Ensure the system supports Create, Read, Update, and Delete operations for both Google Sheets and the database.
   - Maintain data consistency across both platforms.
   
### Optional Challenges (This is not mandatory):
1. Conflict Handling
- Develop a strategy to handle conflicts that may arise when changes are made simultaneously in both Google Sheets and the database.
- Provide options for conflict resolution (e.g., last write wins, user-defined rules).
    
2. Scalability: 	
- Ensure the solution can handle large datasets and high-frequency updates without performance degradation.
- Optimize for scalability and efficiency.

## Submission ⏰
The timeline for this submission is: **Next 2 days**

Some things you might want to take care of:
- Make use of git and commit your steps!
- Use good coding practices.
- Write beautiful and readable code. Well-written code is nothing less than a work of art.
- Use semantic variable naming.
- Your code should be organized well in files and folders which is easy to figure out.
- If there is something happening in your code that is not very intuitive, add some comments.
- Add to this README at the bottom explaining your approach (brownie points 😋)
- Use ChatGPT4o/o1/Github Co-pilot, anything that accelerates how you work 💪🏽. 

Make sure you finish the assignment a little earlier than this so you have time to make any final changes.

Once you're done, make sure you **record a video** showing your project working. The video should **NOT** be longer than 120 seconds. While you record the video, tell us about your biggest blocker, and how you overcame it! Don't be shy, talk us through, we'd love that.

We have a checklist at the bottom of this README file, which you should update as your progress with your assignment. It will help us evaluate your project.

- [✔] My code's working just fine! 🥳
- [ ] I have recorded a video showing it working and embedded it in the README ▶️
- [✔] I have tested all the normal working cases 😎
- [✔] I have even solved some edge cases (brownie points) 💪
- [ ] I added my very planned-out approach to the problem at the end of this README 📜

## Got Questions❓
Feel free to check the discussions tab, you might get some help there. Check out that tab before reaching out to us. Also, did you know, the internet is a great place to explore? 😛

We're available at techhiring@superjoin.ai for all queries. 

All the best ✨.

## Developer's Section

# Google Sheets and Database Sync

This project facilitates real-time synchronization between Google Sheets and a MySQL database. It updates both platforms to ensure consistency, handling data insertion, updates, and deletions efficiently.

## Project Overview

The goal of this project is to keep a Google Sheet and a MySQL database in sync. Changes made to the Google Sheet are reflected in the database, and vice versa. The project is designed to handle the following operations:

1. **Sync Google Sheets to Database**: Updates and inserts new rows in the database based on changes in the Google Sheet.
2. **Sync Database to Google Sheets**: Updates and inserts new rows in the Google Sheet based on changes in the database.

## Approach

1. **Google Sheets to Database Sync**:
    - Fetch data from Google Sheets.
    - Compare fetched data with existing data in the database.
    - Update existing rows, insert new rows, and delete rows in the database based on the comparison.

2. **Database to Google Sheets Sync**:
    - Fetch changes from the database since the last sync.
    - Update the Google Sheet to reflect these changes, including handling row insertions and deletions.
    - Ensure the Google Sheet's data reflects the most recent state of the database.

## Prerequisites

- Python 3.x
- MySQL Database
- Google Sheets API credentials

## Setup

1. **Clone the Repository**:
    ```bash
    git clone <repository-url>
    cd <repository-folder>
    ```

2. **Install Dependencies**:
    Ensure you have the required Python libraries. You can install them using `pip`:
    ```bash
    pip install -r requirements.txt
    ```

3. **Configure Database**:
    Ensure your MySQL database is properly set up and accessible. Update the database connection parameters in `main.py`.

4. **Setup Google Sheets API**:
    - Obtain Google Sheets API credentials and configure the credentials file as specified in `main.py`.

5. **SideNote**
   - Make sure to have your table schema ready in the sql before hand , the code updates the values but does not create the table for the same.

## Other Notes

- The project works on two major keypoints , which is a change log table that accounts for all the changes made on the sql table itself and the other being a last modified time stamp for the sheets , this timestamp is stored on the z1 cell accordingly .For this particular timestamp I wrote an app script that creates a trigger . The function looks like this :
function onEdit(e) {
  // Get the active sheet
  const sheet = e.source.getActiveSheet();
  
  // Choose the cell to store the timestamp
  const timestampCell = sheet.getRange('Z1');  // Adjust if needed
  
  // Set the current timestamp (date and time)
  timestampCell.setValue(new Date());
  
  // Apply a custom date-time format to show both date and time
  timestampCell.setNumberFormat('MM/dd/yyyy HH:mm:ss');  // Adjust format if needed
}
- For the change log table I created a set of triggers that take care of the insertions of operations into itself 
- For any form of updates I compare the two time stamps and have separate functions for changes made to both the sheets and the sql
- A major issue I bumped into was that initially I did not use timestamps for the google sheets , and hence it would always update based on whatever was on the google sheet and overwrite the sql database , disregarding any change made to the database .
- I fixed this issue by introducing the timestamp trigger on the z1 cell .
- some edge cases I hanedled were that I skip incomplete rows as well as duplicated rows as well
- The main function is based on polling , which occurs every 10 seconds , this time limit can be changed .




