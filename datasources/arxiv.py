###############################################################################################################################
# 
# Welcome to the arXiv datasource. This file contains the functions for downloading and analyzing the arXiv dataset.
# 
# INFO: This file heavily relies on the Kaggle arXiv dataset: https://www.kaggle.com/Cornell-University/arxiv
#       If there are any issues with the dataset, please visit the Kaggle website and change this file.
#
###############################################################################################################################

import pandas as pd
import os
import shutil
from functools import partial
from multiprocessing import Pool
from termcolor import colored

from pipeline import pdf2txt

TIMELIMIT = 2*60
STAMP_SEARCH_LIMIT = 1000

PDF2TXT = 'pdf2txt.py'
PDFTOTEXT = 'pdftotext'

RE_REPEATS = r'(\(cid:\d+\)|lllll|\.\.\.\.\.|\*\*\*\*\*)'


def preprocess(arxiv_kaggle_file, arxiv_category=None, arxiv_rows=None):
    # Load the Kaggle arXiv Metadata JSON file
    arxiv_metadata_json = pd.read_json(arxiv_kaggle_file, lines=True, orient="records")
    if arxiv_rows is not None:
        arxiv_metadata_json = arxiv_metadata_json.head(arxiv_rows)
    # Print the number of rows in the arXiv Metadata JSON
    print("Loaded {} entreis from the arXiv metadata JSON file.".format(len(arxiv_metadata_json)))

    if arxiv_category is not None:
        # Split the category string into a list if multiple categories are provided. Else put the category into a list
        if "OR" in arxiv_category:
            arxiv_category = arxiv_category.split(" OR ")
            # Filter the arXiv Metadata JSON by category list with OR logic
            arxiv_metadata_df = arxiv_metadata_json[arxiv_metadata_json["categories"].str.contains("|".join(arxiv_category))]
        elif "AND" in arxiv_category:
            arxiv_category = arxiv_category.split(" AND ")
            # Filter the arXiv Metadata JSON by category list with AND logic
            arxiv_metadata_df = arxiv_metadata_json[arxiv_metadata_json["categories"].str.contains("&".join(arxiv_category))]
        else:
            # Filter the arXiv Metadata JSON by single category
            arxiv_metadata_df = arxiv_metadata_json[arxiv_metadata_json["categories"].str.contains(arxiv_category)]

        # Print the number of rows in the filtered arXiv Metadata df
        print("Filtered out the categories " + str(arxiv_category) + " to a total number of " + str(arxiv_metadata_df["id"].count()) + " entries.")
    else:
        arxiv_metadata_df = arxiv_metadata_json

    if arxiv_metadata_df["id"].count() == 0:
        print(colored("No entries found for the given category. Please check the categories and try again.", "red", attrs=["bold"]))
        exit()
    
    # Save the filtered arXiv Metadata JSON to a JSON file
    arxiv_metadata_df.to_json("arxiv_metadata.json", orient="records", lines=True)

    return arxiv_metadata_df

def process(arxiv_metadata_df, pdf_dir, arxiv_storage_size=None):
    try:
        # Convert the PDFs to text and save them to a JSON file

        globber = os.path.join(pdf_dir, '**/*.pdf') # search expression for glob.glob
        pdffiles = pdf2txt.sorted_files(globber)  # a list of path

        CPU_COUNT = os.cpu_count() - 2
        # CPU_COUNT = 4
        print("Using " + str(CPU_COUNT) + " cores (" + str(os.cpu_count()) + " are available).")
        pool = Pool(CPU_COUNT)
        pool.map(partial(pdf2txt.convert_safe, timelimit=TIMELIMIT), pdffiles)
        pool.close()
        pool.join()

        print("Conversion finished. Now moving the files to the arxiv_txt folder and deleting the PDF files.")

        # Move the TXT files to the arxiv_txt folder and delete the PDF files
        for pdffile in pdffiles:
            txtfile = pdf2txt.reextension(pdffile, 'txt')
            shutil.move(txtfile, "./tmp/arxiv_txt")
            os.remove(pdffile)

        print("Finished moving the files to the arxiv_txt folder and deleting the PDF files.")
        
        # Open all *.txt files from the fulltext folder and save them in a dataframe
        files = [f for f in os.listdir("./tmp/arxiv_txt") if f.endswith(".txt")]

        print("Finished reading the fulltext files.")

        # Store files in a pandas dataframe - name as id and content as text
        file_list = []
        for file in files:
            with open("./tmp/arxiv_txt/" + str(file), "r") as f:
                text = pdf2txt.cleaned_text(f.read())
                file_list.append([str(file), str(text)])
                #arxiv_fulltext_df = arxiv_fulltext_df.append({"id": str(file).str.replace("v\d+", "").str.replace(".txt", ""), "text": str(text)}, ignore_index=True)
        arxiv_fulltext_df = pd.DataFrame(file_list, columns=["id", "text"])

        print("Finished storing the fulltext files into a dataframe.")

        arxiv_fulltext_df["id"] = arxiv_fulltext_df["id"].str.replace(r"v\d+", "", regex=True).str.replace(r".txt", "", regex=True)
        arxiv_fulltext_df.drop_duplicates(subset="id", inplace=True)
        arxiv_metadata_df.drop_duplicates(subset="id", inplace=True)

        print("Finished dropping duplicates.")

        # Merge the two dataframes
        processed_arxiv_df = pd.merge(arxiv_metadata_df, arxiv_fulltext_df, on="id")

        print("Finished merging the two dataframes.")

        # Clean the abstracts in processed_arxiv_df
        processed_arxiv_df["abstract"] = processed_arxiv_df["abstract"].str.replace(r'\s+[a-zA-Z]\s+', ' ', regex=True).str.replace(r'\s+', ' ', regex=True).str.strip()

        print("Finished cleaning the abstracts.")

        processed_arxiv_df.to_json("./arxiv_fulltext.json", orient="records", lines=True)

        print("Finished saving the processed arXiv data to a JSON file.")

        return True
    
    except Exception as e:
        print("Processing failed: " + str(e))
        return False

def download(arxiv_metadata_df, arxiv_storage_size=None, arxiv_process=False):
    # Download the PDFs from the arXiv Metadata JSON

    try:
        # Print the number of rows of id column of the df
        print("Found " + str(arxiv_metadata_df["id"].count()) + " entries.")

        # Filter all papers that have an update_date from 2008 and later - This has to be done since the arXiv ID changed after 2008
        arxiv_metadata_df = arxiv_metadata_df[arxiv_metadata_df["update_date"] >= "2007-01-01"]
        
        # Print the number of rows of id column of the df
        print("Reduced it to " + str(arxiv_metadata_df["id"].count()) + " entries after filtering for update_date > 2007 (IDs are different before that).")

        list_of_gsutil_urls = []

        if not os.path.exists("./tmp/arxiv_pdf"):
            os.makedirs("./tmp/arxiv_pdf")
        if not os.path.exists("./tmp/arxiv_txt"):
            os.makedirs("./tmp/arxiv_txt")

        # Download the PDFs from GCP Bucket by using gsutil cp with gs://arxiv-dataset/arxiv/cs/pdf/[json_id].pdf
        for index, row in arxiv_metadata_df.iterrows():
            # Get the id and the first 4 characters of the id for yymm marker
            pdf_id = row['id']
            pdf_yymm = row["id"][:4]

            # Get the latest version from versions
            pdf_version = row["versions"][-1]["version"]
            list_of_gsutil_urls.append('gs://arxiv-dataset/arxiv/arxiv/pdf/'+pdf_yymm+"/"+pdf_id+pdf_version+'.pdf')
            #print('gs://arxiv-dataset/arxiv/arxiv/pdf/'+pdf_yymm+"/"+pdf_id+pdf_version+'.pdf')

        # Print the number of PDFs to be downloaded
        print("This script will download " + str(len(list_of_gsutil_urls)) + " PDFs from the GCP Bucket.")

        # Save the list_of_gsutil_urls to a txt file
        with open("./tmp/list_of_gsutil_urls.txt", "w") as f:
            for item in list_of_gsutil_urls:
                f.write("%s\n" % item)

        print("Saved the list of gsutil URLs to a txt file.")

        # Download the PDFs from GCP Bucket by using gsutil cp with gs://arxiv-dataset/arxiv/cs/pdf/[json_id].pdf
        os.system("gsutil -m cp -I ./tmp/arxiv_pdf/ < ./tmp/list_of_gsutil_urls.txt > /dev/null 2>&1")

        if arxiv_process:
            # Process the PDFs
            is_processed = process(arxiv_metadata_df, "./tmp/arxiv_pdf")

            # Delete the PDFs
            os.system("rm -rf ./tmp/arxiv_pdf")

            # Delete the TXT files
            os.system("rm -rf ./tmp/list_of_gsutil_urls.txt")
            os.system("rm -rf ./tmp/arxiv_txt")

            # Delete the tmp folder
            os.system("rm -rf ./tmp")

            # Save the processed arXiv Metadata JSON to a JSON file
            # processed_arxiv_df.to_json("arxiv_fulltext.json")

        return is_processed

    except Exception as e:
        print("Error: " + str(e))
        return False
    
def process_local_pdfs(arxiv_metadata_df, arxiv_local_PDF_dir):    
    if not os.path.exists("./tmp/arxiv_txt"):
        os.makedirs("./tmp/arxiv_txt")

    is_processed = process(arxiv_metadata_df, arxiv_local_PDF_dir)

    return is_processed