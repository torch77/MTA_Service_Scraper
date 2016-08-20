"""Frame work for testing out scraping of MTA Service Status
Do this eventually using OOP to practice
Also set up to insert data into postgresql database on AWS
Add error catching so that script will keep running on AWS if there is error with retrival from web"""

# http://web.mta.info/status/serviceStatus.txt
# Libraries
from bs4 import BeautifulSoup
import requests
import sqlite3
import datetime

#### To DO
# add ability to parse text for service delays/planned work
# new class to write to database
# fix status hour issue, maybe just make it a text field with no look up
# look into scheduling
# more error/anamolous case catching, don't just crash
#


# function to create bs4 object
def get_soup(url):
    # get website
    resp = requests.get(url)

    # check status and return if ok, else error
    if resp.status_code == 200:
        bs_obj = BeautifulSoup(resp.text, "xml")
        return bs_obj
    else:
        resp.raise_for_status()


# function to parse MTA data
def mta_parse(xml_obj):
    # get response code, not sure what this means
    response_code = xml_obj.find("responsecode").get_text()
    print("Response Code: " + response_code)

    # check response code, bail if not 0
    if response_code != "0":
        return None

    ok_status = "GOOD SERVICE"

    # lists to hold status info
    sub_info = []
    bus_info = []
    bt_info = []
    lirr_info = []
    mnr_info = []

    # get date and time of request, doesn't change
    now = datetime.datetime.now()
    date = now.strftime("%Y%m%d")
    hour = now.hour

    # get subway statuses by line
    for line in xml_obj.subway.find_all("line"):
        name = line.find("name").get_text()
        status = line.find("status").get_text()
        if status != ok_status:
            status_text = line.find("text").string
            status_date = line.find("Date").get_text()
            status_hour = line.find("Time").get_text()
        else:
            status_text = None
            status_date = None
            status_hour = None

        # store line info
        sub_info.append({"Service": "Subway", "Name": name, "Req_Date": date, "Req_Hour": hour, "Status": status,
                         "Status_Text": status_text, "Status_Date": status_date, "Status_Hour": status_hour})

    # get bus statuses
    for line in xml_obj.bus.find_all("line"):
        name = line.find("name").get_text()
        status = line.find("status").get_text()
        if status != ok_status:
            status_text = line.find("text").string
            status_date = line.find("Date").get_text()
            status_hour = line.find("Time").get_text()
        else:
            status_text = None
            status_date = None
            status_hour = None

        # store line info
        bus_info.append({"Service": "Bus", "Name": name, "Req_Date": date, "Req_Hour": hour, "Status": status,
                         "Status_Text": status_text, "Status_Date": status_date, "Status_Hour": status_hour})


    # get B+T statuses
    for line in xml_obj.BT.find_all("line"):
        name = line.find("name").get_text()
        status = line.find("status").get_text()
        if status != ok_status:
            status_text = line.find("text").string
            status_date = line.find("Date").get_text()
            status_hour = line.find("Time").get_text()
        else:
            status_text = None
            status_date = None
            status_hour = None

        # store line info
        bt_info.append({"Service": "B_T", "Name": name, "Req_Date": date, "Req_Hour": hour, "Status": status,
                        "Status_Text": status_text, "Status_Date": status_date, "Status_Hour": status_hour})

    # get LIRR statuses
    for line in xml_obj.LIRR.find_all("line"):
        name = line.find("name").get_text()
        status = line.find("status").get_text()
        if status != ok_status:
            status_text = line.find("text").string
            status_date = line.find("Date").get_text()
            status_hour = line.find("Time").get_text()
        else:
            status_text = None
            status_date = None
            status_hour = None

        # store line info
        lirr_info.append({"Service": "LIRR", "Name": name, "Req_Date": date, "Req_Hour": hour, "Status": status,
                          "Status_Text": status_text, "Status_Date": status_date, "Status_Hour": status_hour})

    # get MNR statuses
    for line in xml_obj.MetroNorth.find_all("line"):
        name = line.find("name").get_text()
        status = line.find("status").get_text()
        if status != ok_status:
            status_text = line.find("text").string
            status_date = line.find("Date").get_text()
            status_hour = line.find("Time").get_text()
        else:
            status_text = None
            status_date = None
            status_hour = None

        # store line info
        mnr_info.append({"Service": "MNR", "Name": name, "Req_Date": date, "Req_Hour": hour, "Status": status,
                         "Status_Text": status_text, "Status_Date": status_date, "Status_Hour": status_hour})

    # return lists
    return [sub_info, bus_info, bt_info, lirr_info, mnr_info]


# function to update sqlite database, should make into its own class
def update_db(service_info):

    # assume db is in same dir
    db_name = "MTA_Service.sqlite"


    # add a function with proper error catching to open db
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    # check that list exists
    if service_info is not None:
        # iterate over service dictionaries and then lines to update db
        for service in service_info:
            for line in service:
                # get information from dictionary
                service = line.get("Service")
                name = line.get("Name")
                req_date = line.get("Req_Date")
                req_hour = int(line.get("Req_Hour")) + 1 # add hour b/c it is references hour table and can't start at 0
                status = line.get("Status")
                text = line.get("Status_Text")
                status_date = line.get("Status_Date")
                # check status hour, if none then return none
                if line.get("Status_Hour") == None:
                    status_hour = None
                else:
                    status_hour = datetime.datetime.strptime(line.get("Status_Hour").strip(), "%I:%M%p").hour + 1
                # set this to None for now b/c we need to parse string, MTA has space instead of zero padding for hour
                # datetime.datetime.strptime(" 1:02AM", " %I:%M%p")
                # status_hour = int(line.get("Status_Hour")) + 1 # add hour b/c it is references hour table and can't start at 0


                # get status and name pk's
                line_pk = get_line_pk(service, name)
                status_pk = get_status_pk(status)

                # create values dict
                values = {"Record": None, "Line_ID": line_pk, "Status_ID": status_pk, "Req_Date": req_date,
                          "Req_Hour": req_hour, "Text": text, "Status_Date": status_date, "Status_Hour": status_hour}

                # print("Inserting " + str(values))
                try:
                    c.execute("INSERT INTO Status_Record (Record_ID, Line_ID, Status_ID, Req_Date, Req_Hour, "
                              "Status_Text, Status_Date, Status_Hour) VALUES (:Record, :Line_ID, :Status_ID, :Req_Date, "
                              ":Req_Hour, :Text, :Status_Date, :Status_Hour)", values)
                    print("Insert " + line.get("Name") + " complete")
                except sqlite3.Error as e:
                    print(e)

            # commit for every service
            try:
                conn.commit()
            except sqlite3.Error as e:
                print(e)


        #close connection
        conn.close()
    else:
        print("No List Returned from XML, Check Response Code")


# function to map out line PK's
def get_line_pk(service, line_name):

    # dictionaries to pull pk from
    subway_switch = {"123": 1, "456": 2, "7": 3, "ACE": 4, "BDFM": 5, "G": 6, "JZ": 7, "L": 8,
                     "NQR": 9, "S": 10, "SIR": 11}
    bus_switch = {"B1 - B84": 12, "B100 - B103": 13, "BM1 - BM5": 14, "BX1 - BX55": 15, "BXM1 - BXM18": 16,
                  "M1 - M116": 17, "Q1 - Q113": 18, "QM1 - QM44": 19, "S40 - S98": 20, "x1 - x68": 21}
    bt_switch = {"Bronx-Whitestone": 22, "Cross Bay": 23, "Henry Hudson": 24, "Hugh L. Carey": 25, "Marine Parkway": 26,
                 "Queens Midtown": 27, "Robert F. Kennedy": 28, "Throgs Neck": 29, "Verrazano-Narrows": 30}
    lirr_switch = {"Babylon": 31, "City Terminal Zone": 32, "Far Rockaway": 33, "Hempstead": 34, "Long Beach": 35,
                   "Montauk": 36, "Oyster Bay": 37, "Port Jefferson": 38, "Port Washington": 39, "Ronkonkoma": 41,
                   "West Hempstead": 42}
    mnr_switch = {"Hudson": 42, "Harlem": 43, "Wassaic": 44, "New Haven": 45, "New Canaan": 46, "Danbury": 47,
                  "Waterbury": 48, "Pascack Valley": 49, "Port Jervis": 50}

    # check service, return none if no match
    if service.lower() == "subway":
        return subway_switch.get(line_name, None)
    elif service.lower() == "bus":
        return bus_switch.get(line_name, None)
    elif service.lower() == "b_t":
        return bt_switch.get(line_name, None)
    elif service.lower() == "lirr":
        return lirr_switch.get(line_name, None)
    elif service.lower() == "mnr":
        return mnr_switch.get(line_name, None)
    else:
        return None


def get_status_pk(status):
    # dictionaries to pull pk from
    status_switch = {"GOOD SERVICE": 1, "SERVICE CHANGE": 2, "PLANNED WORK": 3, "DELAYS": 4}
    # check status, return none if no match
    return status_switch.get(status, None)


def main():

    url = "http://web.mta.info/status/serviceStatus.txt"

    site = get_soup(url)

    service_info = mta_parse(site)

    update_db(service_info)


main()


