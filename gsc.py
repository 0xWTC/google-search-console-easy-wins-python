# Get data from Google Search Console for a website.
# Use google-searchconsole
# Analyze the data with Pandas
# Save the data to an Excel file usins Pandas ExcelWriter

import searchconsole
import pandas
import pathlib
from datetime import datetime

def authenticate():
    # Authenticate to Google Search Console
    # check if is credentials.json exists in current directory
    if "credentials.json" in pathlib.Path.cwd().glob("**/*"):
        print("credentials.json not found in current directory")
        account = searchconsole.authenticate(
            client_config="client_secrets.json", serialize="credentials.json"
        )


def generate_dfs_list(df, property):

    # extract the domain name and the tld from the property
    domain = property.split(":")[1]

    # filter the query column in the dataframe with regex for questions
    df_questions = df[
        df["query"].str.contains(
            "^(who|what|where|when|why|how, was, did, do, is, are, aren't, will, won't, does, if)[\" \"]"
        )
    ]

    # filter the query column in the dataframe with regex for answers for longtails (8+ words)
    df_longtails = df[df["query"].str.contains('([^" "]*\s){7,}?')]
    # filter for 12+ words
    df_longtails_12 = df[df["query"].str.contains('([^" "]*\s){11,}?')]

    # all keywords
    print("\nall keywords with positions 5-10")
    keywords5 = df[df["position"].between(5, 10)]
    print(keywords5.head(20))
    print("\nall keywords with positions 10-20")
    keywords10 = df[df["position"].between(10, 20)]
    print(keywords10.head(20))
    # question keywords
    print("\nquestion keywords with positions 5-10")
    questions_keywords5 = df_questions[df_questions["position"].between(5, 10)]
    print(questions_keywords5.head(20))
    print("\nquestion keywords with positions 10-20")
    questions_keywords10 = df_questions[df_questions["position"].between(10, 20)]
    print(questions_keywords10.head(20))
    # longtail keywords
    print("\nlongtail keywords in positions 5-10")
    longtails_keywords5_8 = df_longtails[df_longtails["position"].between(5, 10)]
    print(longtails_keywords5_8.head(20))
    print("\nlongtail keywords in positions 10-20")
    longtails_keywords10_8 = df_longtails[df_longtails["position"].between(10, 20)]
    print(longtails_keywords10_8.head(20))
    # longtail 12+ keywords
    print("\nlongtail keywords in positions 5-10")
    longtails_keywords5_12 = df_longtails_12[df_longtails_12["position"].between(5, 10)]
    print(longtails_keywords5_12.head(20))
    print("\nlongtail keywords in positions 10-20")
    longtails_keywords10_12 = df_longtails_12[df_longtails_12["position"].between(10, 20)]
    print(longtails_keywords10_12.head(20))

    # gather all new dataframes to a dictionary
    dfs_dict = {
        "Questions 5-10": questions_keywords5,
        "Questions 5-20": questions_keywords10,
        "Longtails 5-10 12+W": longtails_keywords5_12,
        "Longtails 10-20 12+W": longtails_keywords10_12,
        "Longtails 5-10 8+W": longtails_keywords5_8,
        "Longtails 10-20 8+W": longtails_keywords10_8,
        "All Keywords 5-10": keywords5,
        "All Keywords 10-20": keywords10,
    }

    # save the dataframes to an excel file in a folder called data in a subfolder called after the domain name and each file is named after the datestamp
    # create a folder called data in the current directory
    pathlib.Path("data").mkdir(parents=True, exist_ok=True)
    # create a subfolder called after the domain name
    pathlib.Path("data/" + domain).mkdir(parents=True, exist_ok=True)
    # create a datestamp with the current date and time
    datestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    # create a file name
    filename = "data/" + domain + "/" + datestamp + ".xlsx"
    # create a Pandas ExcelWriter using the filename
    writer = pandas.ExcelWriter(filename, engine="xlsxwriter")

    # write the dataframes to one excel file with the Pandas ExcelWriter. Each sheet is a dataframe with it's own sheet name derived from the dictionary key name.
    for key, value in dfs_dict.items():
        value.to_excel(writer, sheet_name=key, index=False)
        # set column A width to 60
        writer.sheets[key].set_column("A:A", 60)
    # close the Pandas ExcelWriter
    writer.save()


def query_gsc(property, lookback_days=90, sort_by=["impressions"]):
    # Get data from Google Search Console
    account = searchconsole.authenticate(
        client_config="client_secrets.json", credentials="credentials.json"
    )

    # to print available properties and their IDs
    print("\nAvailable properties:")
    print(account.webproperties)

    webproperty = account[property]

    # build the dataframe
    df = (
        webproperty.query.range("today", days=-abs(lookback_days))
        .dimension("query")
        .get()
        .to_dataframe()
    )

    # sort the dataframe
    df = df.sort_values(by="impressions", ascending=False)

    # print the dataframe including the top 10 queries and all columns
    print(df.head(20))

    # generate a list of dfs
    generate_dfs_list(df, property)


def main(property, lookback_days):
    # Authenticate to Google Search Console
    authenticate()
    # Get data from Google Search Console
    query_gsc(property, lookback_days)


if __name__ == "__main__":
    # run the main function
    main(property="sc-domain:yogakali.com", lookback_days=90)
