# Cleaning WAPO police shooting data and uploading to google sheets
import pandas as pd
import gspread as gspread
from df2gspread import df2gspread as df2g
from oauth2client.service_account import ServiceAccountCredentials

# Downloading Data and Prepping for Upload
df = pd.read_csv('https://raw.githubusercontent.com/washingtonpost/data-police-shootings/master/fatal-police-shootings-data.csv')

# Creating Column of Age Bins for Histogram
df['age_bin'] = df.age
bins = []
for i,row in df.iterrows():
    if row.age < 18:
        bins.append('Minor')
    elif row.age > 17 and row.age < 25:
        bins.append('18-24')
    elif row.age > 24 and row.age < 31:
        bins.append('25-30')
    elif row.age > 30 and row.age < 36:
        bins.append('31-35')
    elif row.age > 35 and row.age < 41:
        bins.append('36-40')
    elif row.age > 40 and row.age < 46:
        bins.append('41-45')
    elif row.age > 45 and row.age < 51:
        bins.append('46-50')
    elif row.age > 50 and row.age < 56:
        bins.append('51-55')
    elif row.age > 55 and row.age < 61:
        bins.append('56-60')
    elif row.age > 60:
        bins.append('60+')
    else:
        bins.append('Unknown')

df.age_bin = bins

# Converting Race & Gender Abbreviations to Full Name
df.race = df.race.replace({'W':'White', 'H':'Hispanic', 'B':'Black', 'A':'Asian', 'N':'Native American', 'O':'Other'})
df.gender = df.gender.replace({'M':'Male', 'F':'Female'})

# Handling Nulls
df = df.fillna('Unknown')

# Creating Year & Month Columns
df.date = pd.to_datetime(df.date)
df['Year'] = df.date.dt.year
df['Month'] = df.date.dt.month_name()
df['Month_Abbrev'] = df.Month.apply(lambda x:x[:3])
df['Month_Num'] = df.date.dt.month
df.date = pd.to_datetime(df.date).dt.date  # removing timestamp

# Replacing single anomaly in Gender col (missing gender)
df.loc[(df.id == 2956) & (df.name == 'Scout Schultz'), 'gender'] = 'Male'

# Uploading df to Google Sheets
# Resources: https://techwithtim.net/tutorials/google-sheets-python-api-tutorial/ & https://stackoverflow.com/questions/59117810/changing-column-format-in-google-sheets-by-gspread-and-google-sheets-api
scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("creds.json", scope)
client = gspread.authorize(creds)
sheet_key = "1xn4Wori5gD8j5U51c1OUeiHaDuRqRIq0Php66hUh2d0"
sheet_name = "Sheet1"
df2g.upload(df, sheet_key, sheet_name, credentials=creds, row_names=False)


"""# Editing column data format (from text to number)
spreadsheet = client.open("Police Shootings").sheet1

requests = [{
    "repeatCell": {
        "range": {
            "startRowIndex":1,
            "startColumnIndex": 18,
            "endColumnIndex": 19
        },
        "cell": {
            "userEnteredFormat": {
                "numberFormat": {
                    "type": "NUMBER"
                }
            }
        },
        "fields": "userEnteredFormat.numberFormat"
    }
}]

body = {
    'requests': requests
}

res = spreadsheet.batch_update(body)
print(res)"""