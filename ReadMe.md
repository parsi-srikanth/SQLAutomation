# SQLAutomation

## Introduction
The SQLAutomation project automates SQL query execution and stores results in CSV or Excel formats for streamlined analysis. By eliminating manual data handling, it saves time, ensures accuracy, and enables faster decision-making processes. With SQLAutomation, organizations can focus on deriving insights from data rather than on repetitive data processing tasks.

## How to Install and Execute the Project
Below are the steps to install and execute the SQLAutomation project:

1. Clone the repository:
   ```git clone https://github.com/your-username/SQLAutomation.git```

2. Set up a Python virtual environment:
   ```
   python -m venv sql-automation-venv --prompt="sql-automation"
   ```

3. Navigate to the virtual environment directory:
   ```
   cd sql-automation-venv/Scripts
   ```

4. Activate the virtual environment:
   ```
   activate
   ```

5. Navigate back to the SQLAutomation directory.

6. Install the required dependencies using `requirements.txt`:
   ```
   python -m pip install -r requirements.txt 
   ```

7. update the parameters in `generate_config.py`.

   Note: If you add a new package or update an existing one, update the `requirements.txt` file:
   ```
   python -m pip freeze > requirements.txt
   ```
## How to run server

8. Execute 
    ```
    python generate_config.py
    ```

9. Execute the server Python script:
   ```
   python server.py
   ```

10. Store the jinja templates/ sql queries in sql_files folder.

By following these steps, you can automate SQL query execution and save data to excel or csv files with SQLAutomation.

## How to run client

11. Update the generate_config.py > [client] section with appropriate data for sql_template.

12. Execute in new terminal.
    ```
    python generate_config.py
    ```

13. Execute the client script.
   ```
   python client.py
   ```
**For each new sql query steps 11 - 13 needs to be done.**

## How to trigger client via shell script.

14. Select the required sql file from the **sql_files** directory in the project structure.
15. Drag and drop this file onto the sql_client.bat file in the project root directory.

## Error Resoultion:
If you face the below error, then go to detabase_helper.py class and uncomment line 15.
**Error:** cx_Oracle.DatabaseError: DPI-1047: Cannot locate a 64-bit Oracle Client library: "The specified module could not be found".

a. Download the oracle instant client for your system from "https://www.oracle.com/database/technologies/instant-client/downloads.html".

b. Give the path from you installation in line 15.

c. Run the code now and check.

