# Get data from Google Search Console for a website.
# Use google-searchconsole
# Analyze the data with Pandas
# Save the data to an Excel file using Pandas ExcelWriter

import searchconsole
import pandas
import pathlib
from datetime import datetime
from tqdm import tqdm
from newspaper import Article, Config
import cloudscraper
import newspaper


def get_article(url):
    '''
    Get the article from the url
    Use newspaper3k
    Return the article title and text
    Use cloudscraper to bypass Cloudflare
    '''
    try:
        print(f"Getting {url}")
        scraper = cloudscraper.create_scraper()
        html = scraper.get(url).content
        article = newspaper.Article(url=" ")
        article.set_html(html)
        article.parse()
        article.nlp()

        # count words in article.text
        word_count = len(article.text.split())

        # create a dictionary that contains the article title, text, and the word_count
        article_dict = {
            "title": article.title,
            "text": article.text,
            "word_count": word_count
        }

        return article_dict

    except Exception as error:
        print(error)
        return ("", "")

def authenticate():
    # Authenticate to Google Search Console
    # check if credentials.json doesn't exist in current directory
    if not pathlib.Path("credentials.json").is_file():
        print("credentials.json not found in current directory")
        # if it doesn't exist, create it
        searchconsole.authenticate(
            client_config="client_secrets.json", credentials="credentials.json"
        )
    else:
        print("credentials.json found in current directory")

    
def confirm_authentication(domain):
    # Get data from Google Search Console
    account = searchconsole.authenticate(
        client_config="client_secrets.json", credentials="credentials.json"
    )

    # to print available properties and their IDs
    # print("\nAvailable properties:")
    # print(account.webproperties)
    # concat 'sc-domain:' + domain
    property = "sc-domain:" + domain

    # assign the property ID to a variable
    webproperty = account[property]

    return webproperty

def generate_dfs_list(df, domain, page):
    # filter the query column in the dataframe with regex for questions
    df_questions = df[
        df["query"].str.contains(
            "^(who|what|where|when|why|how|was|did|do|is|are|aren't|won't|does|if|can|could|should|would|who|what|where|when|why|will|did|do|is|are|won't|were|weren't|shouldn't|couldn't|cannot|can't|didn't|did not|does|doesn't|wouldn't)[\" \"]"
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
    print("\nall keywords with positions 20-100")
    keywords20 = df[df["position"].between(20, 100)]
    print(keywords20.head(20))
    # question keywords
    print("\nquestion keywords with positions 5-10")
    questions_keywords5 = df_questions[df_questions["position"].between(5, 10)]
    print(questions_keywords5.head(20))
    print("\nquestion keywords with positions 10-20")
    questions_keywords10 = df_questions[df_questions["position"].between(10, 20)]
    print(questions_keywords10.head(20))
    print("\nquestion keywords with positions 20-100")
    questions_keywords20 = df_questions[df_questions["position"].between(20, 100)]
    print(questions_keywords20.head(20))
    # longtail keywords
    print("\nlongtail keywords in positions 5-10")
    longtails_keywords5_8 = df_longtails[df_longtails["position"].between(5, 10)]
    print(longtails_keywords5_8.head(20))
    print("\nlongtail keywords in positions 10-20")
    longtails_keywords10_8 = df_longtails[df_longtails["position"].between(10, 20)]
    print(longtails_keywords10_8.head(20))
    print("\nlongtail keywords in positions 20-100")
    longtails_keywords20_8 = df_longtails[df_longtails["position"].between(20, 100)]
    print(longtails_keywords20_8.head(20))
    # longtail 12+ keywords
    print("\nlongtail keywords in positions 5-10")
    longtails_keywords5_12 = df_longtails_12[df_longtails_12["position"].between(5, 10)]
    print(longtails_keywords5_12.head(20))
    print("\nlongtail keywords in positions 10-20")
    longtails_keywords10_12 = df_longtails_12[
        df_longtails_12["position"].between(10, 20)
    ]
    print(longtails_keywords10_12.head(20))
    print("\nlongtail keywords in positions 20-100")
    longtails_keywords20_12 = df_longtails_12[
        df_longtails_12["position"].between(20, 100)
    ]
    print(longtails_keywords20_12.head(20))
    # all keywords
    print("\nall keywords")
    all_keywords = df
    print(all_keywords.head(20))

    # gather all new dataframes to a dictionary
    dfs_dict = {
        "All Keywords": all_keywords,
        "Questions 5-10": questions_keywords5,
        "Questions 10-20": questions_keywords10,
        "Questions 20-100": questions_keywords20,
        "Longtails 5-10 8+W": longtails_keywords5_8,
        "Longtails 10-20 8+W": longtails_keywords10_8,
        "Longtails 20-100 8+W": longtails_keywords20_8,
        "Longtails 5-10 12+W": longtails_keywords5_12,
        "Longtails 10-20 12+W": longtails_keywords10_12,
        "Longtails 20-100 12+W": longtails_keywords20_12,
        "All Keywords 5-10": keywords5,
        "All Keywords 10-20": keywords10,
        "All Keywords 20-100": keywords20,
    }

    # save the dataframes to an excel file in a folder called data in a subfolder called after the domain name and each file is named after the datestamp
    # create a datestamp with the current date and time
    datestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

    # replace all "/" in domain with "-"
    slug = page.replace("/", "-")
    slug = slug.replace("https:--", "-")
    slug = slug.replace("http:--", "-")
    # remove starting dash from slug
    slug = slug.lstrip("-")

    if page == "all-pages":
        slug = domain + "-all-queries-"

    # create a file name
    filename = "data/" + domain + "/" + slug + "-" + datestamp + ".xlsx"
    filename = filename.replace("--", "-")

    # create a Pandas ExcelWriter using the filename. The first column 'page' is filled with urls but should be written as strings, not clickable urls.
    writer = pandas.ExcelWriter(filename, engine='xlsxwriter', options={'strings_to_urls': False})

    # write the dataframes to one excel file with the Pandas ExcelWriter. Each sheet is a dataframe with it's own sheet name derived from the dictionary key name.
    for key, value in dfs_dict.items():
        value.to_excel(writer, sheet_name=key, index=False)
        # set column widths
        writer.sheets[key].set_column("A:A", 90)
        writer.sheets[key].set_column("B:B", 30)
        writer.sheets[key].set_column("D:D", 30)
        writer.sheets[key].set_column("G:G", 30)
        writer.sheets[key].set_column("H:H", 30)
        writer.sheets[key].set_column("I:I", 90)
    # close the Pandas ExcelWriter
    writer.save()


def gsc_queries(domain, page, lookback_days=90, sort_by=["impressions"]):

    webproperty = confirm_authentication(domain)


    df = (
        webproperty.query.range("today", days=-abs(lookback_days))
        .dimension("query")
        .filter('page', page, 'equals')
        .get()
        .to_dataframe()
    )

    # sort the dataframe
    df = df.sort_values(by="impressions", ascending=False)

    # round all numbers
    df = df.round(1)

    # print the dataframe including the top 10 queries and all columns
    # print(df.head(20))

    if page != "all-pages":
        try:
            article_dict = get_article(page)
            # create a column in df called "exists_on_site". Populate with the number of query strings found in article_dict[title] and article_dict[text]. comparison is case insensitive. use iterrows() to iterate over the rows of the dataframe and tqdm to show a progress bar.
            for index, row in tqdm(df.iterrows(), total=len(df)):
                df.at[index, "exists_on_site"] = 0
                if row["query"].casefold() in article_dict["title"].casefold():
                    df.at[index, "exists_on_site"] += 1
                # count number of times query.casefold() is found in article_dict[text].casefold()
                df.at[index, "exists_on_site"] += article_dict["text"].casefold().count(row["query"].casefold())
                
            # add a column to df called "url" and populate it with the value from the column "page". Insert it as the first column in the dataframe.
            df.insert(0, "page", page) 

            # add a column to the end of df called "word_count" and populate it with article_dict[word_count]
            df.insert(len(df.columns), "word_count", article_dict["word_count"])

            # add a column to df called "title" and populate it with article_dict[title]
            df.insert(len(df.columns), "title", article_dict["title"])

        except Exception as error:
            print(error)

    return df

def gsc_pages(domain, lookback_days=90, sort_by=["impressions"]):

    webproperty = confirm_authentication(domain)

    # build the dataframe
    df = (
        webproperty.query.range("today", days=-abs(lookback_days))
        .dimension("page")
        .get()
        .to_dataframe()
    )

    # sort the dataframe
    df = df.sort_values(by="impressions", ascending=False)

    # get all the values from df from the column 'page' to a list
    pages = df["page"].tolist()


    # round all numbers
    df = df.round(1)

    # print 20 from df
    # print(df.head(20))


    # write the df to .xlsx file
    datestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    df.to_excel("data/" + domain + "/" + domain + "-pages-" + datestamp + ".xlsx", index=False)
    
    # return the list of pages
    return pages


def main(property, lookback_days):
    # Authenticate to Google Search Console
    authenticate()

    # create a folder called data in the current directory
    pathlib.Path("data").mkdir(parents=True, exist_ok=True)
    # create a subfolder called after the domain name
    pathlib.Path("data/" + property).mkdir(parents=True, exist_ok=True)

    # Get the pages from Google Search Console
    pages = gsc_pages(property, lookback_days)

    # create an empty dataframe
    df_queries = pandas.DataFrame()

    # Get the queries for each page from Google Search Console. use tqdm to show a progress bar
    for page in tqdm(pages):
        try:
            new_df = gsc_queries(property, page, lookback_days)
            df_queries = pandas.concat([df_queries, new_df])
        except Exception as e:
            # sometimes GSC doesn't actually have the query data for a page that just gets a few impressions.
            print(e)
            continue

    # sort the dataframe by the column "impressions"
    df_queries = df_queries.sort_values(by="impressions", ascending=False)
    
    generate_dfs_list(df_queries, property, "all-pages")


if __name__ == "__main__":
    # run the main function. ask the user for the domain name and the lookback days
    main(input("Enter the domain name without https/www: "), int(input("Enter the lookback days: ")))

