import json
import logging
import os
import time
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials


def get_creds():
    # load credentials from repository secrets if run from job, else load from local file
    CHURCH_JSON = os.environ.get("CHURCH_JSON")
    if CHURCH_JSON:
        church_creds = json.loads(CHURCH_JSON)
    else:
        with open("church_creds.json") as f:
            church_creds = json.load(f)

    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/drive",
    ]

    return ServiceAccountCredentials.from_json_keyfile_dict(church_creds, scope)


def make_callings_data(form_data: list) -> pd.DataFrame:
    simplified_name_map = {
        "What is the Name of the Proposed Calling (please refer to General Handbook (https://www.churchofjesuschrist.org/study/manual/general-handbook/30-callings-in-the-church?lang=eng#title_number125) and Callings and Trainings (https://www.churchofjesuschrist.org/callings?lang=eng) for a reference on duties and authorized names for callings)?": "Calling",
        "Full Name of the Person Proposed for the Calling:": "PersonToCall",
        "Will this Require they be Released from a Current Calling?": "ReleaseRequired",
        "Name of the Proposed Organization to which the Person would be Called:": "Organization",
        "Once Again, Will this Require they be Released from a Current Calling?": "ReleaseRequired2",
        "If You Know, from which Organization are you Requesting this Person?": "CurrentOrganization",
        "Name of Person Submitting the Form": "FormSubmittedBy",
        "Ideal Date for Proposed Person to Begin Service": "IdealStartDate",
        "Does this Proposal Require Someone Else be Released from the Calling You are Asking to Fill?": "ReleaseCurrentlyServing",
        "Name of the Person Who Needs to be Released from this Calling:": "CurrentlyServing",
        "Is this Person Moving?": "Moving?",
        "Date they are Moving:": "MovingDate",
        "Any Additional Comments:": "Comments",
        "Calling Approval ": "BishopApproval",
        "Calling extended and accepted": "ExtendedAndAccepted",
    }

    calling_df = pd.DataFrame(form_data[1:], columns=form_data[0]).rename(
        columns=simplified_name_map
    )

    # add some new columns to use later
    calling_df["ScheduledMeetingWith"] = ""
    calling_df["DateRequested"] = pd.to_datetime(calling_df["Timestamp"]).dt.strftime(
        "%m/%d/%Y"
    )
    calling_df["cleaned_approval_text"] = (
        calling_df["BishopApproval"].str.strip().str.lower().str[0]
    )
    return calling_df


def get_filtered_data(calling_df: pd.DataFrame) -> pd.DataFrame:
    keeps = [
        "DateRequested",
        "PersonToCall",
        "Calling",
        "Organization",
        "FormSubmittedBy",
        "IdealStartDate",
        "BishopApproval",
        "ExtendedAndAccepted",
        "Sustained",
        "Set Apart",
    ]

    # filter out anything the bishop hasn't approved (needs first letter to by Y)
    # filter out anything that has already been recorded in LCR
    progress_df = calling_df.loc[
        (calling_df["cleaned_approval_text"] == "y") & (calling_df["Recorded"] == ""),
        keeps,
    ]
    return progress_df


def empty_sheet(sheet):
    sheet_values = sheet.get_values()
    sheet_df = pd.DataFrame(sheet_values[1:], columns=sheet_values[0])

    # remove the current data in the sheet
    eraser = sheet_df.copy()
    for c in eraser.columns:
        eraser[c] = ""
    sheet.update([eraser.columns.values.tolist()] + eraser.values.tolist())
    return


def trim_logs():
    with open("log.txt") as f:
        logs = f.readlines()

    # keep only 3 weeks of logs
    if len(logs) >= 21:
        logs = logs[-20:]  # will have current log entry added later

    with open("log.txt", "w") as f:
        logs = f.writelines(logs)


def add_log_to_readme():
    # get readme content and log content
    with open("README.md") as f:
        readme = f.readlines()

    with open("log.txt") as f:
        logs = f.readlines()

    # get most recent run details and format them
    most_recent_log_entry = [l.strip() for l in logs[-1].split("  ")]

    del most_recent_log_entry[1]
    update = "- " + " UTC: ".join(most_recent_log_entry)

    # update and rewrite readme contents
    readme[-1] = update
    with open("README.md", "w") as f:
        f.writelines(readme)


def main():

    LOG_FILENAME = "log.txt"
    LOG_FORMAT = "%(asctime)s  %(levelname)-6s %(message)s"

    logging.basicConfig(filename=LOG_FILENAME, level=logging.INFO, format=LOG_FORMAT)
    logger = logging.getLogger(__name__)

    try:

        creds = get_creds()
        client = gspread.authorize(creds)

        # access calling form submissions
        form_submissions = client.open(
            "2022-4-10 Provo Peak 9th Ward Calling Submission (Responses)"
        ).sheet1
        calling_df = make_callings_data(form_submissions.get_values())

        updated_df = get_filtered_data(calling_df)

        # open destination sheet
        progress_sheet = client.open("Calling_Approval_Progress").sheet1

        # delete current data
        empty_sheet(progress_sheet)

        # add new data
        progress_sheet.update(
            [updated_df.columns.values.tolist()] + updated_df.values.tolist()
        )

        logger.info("Job successfully completed")
    except:
        logger.error("Job failed")

    # trim logs and update readme
    trim_logs()
    add_log_to_readme()

    return


if __name__ == "__main__":
    main()
