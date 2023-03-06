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
import re
import shutil
import glob
import shlex
from functools import partial
import nltk
nltk.download('words', quiet=True)
words = set(nltk.corpus.words.words())

from multiprocessing import Pool
from subprocess import check_call, CalledProcessError, TimeoutExpired, PIPE

from helpers import fixunicode

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
    if arxiv_category is not None:
        # Filter the arXiv Metadata JSON by category
        arxiv_metadata_df = arxiv_metadata_json[arxiv_metadata_json["categories"].str.contains(arxiv_category)]
    else:
        arxiv_metadata_df = arxiv_metadata_json
    
    # Save the filtered arXiv Metadata JSON to a JSON file
    arxiv_metadata_df.to_json("arxiv_metadata.json", orient="records", lines=True)

    return arxiv_metadata_df

# def cleaned_text(text):
#     """
#     Clean the text of the PDFs by removing several things
#     """
#     # Find the first appearance of the word "Introduction" and delete everything before it. Make it case insensitive.
#     text = re.sub(r'(?i)^(.*?Introduction)', '', text)
#     text = re.sub(r'(?i)^(.*?INTRODUCTION)', '', text)

#     # Clean the text
#     text = re.sub(r'\s+', ' ', text)

#     # Remove every parenthesis, square bracket, and curly bracket
#     text = re.sub(r'[\(\)\[\]\{\}]', '', text)

#     # Remove special characters
#     # text = re.sub(r'[^a-zA-Z0-9\s]', '', text)

#     # Remove all numbers from the text
#     text = re.sub(r'\d+', '', text)

#     # Remove hyperlinks from the text
#     text = re.sub(r'https?://\S+', '', text)

#     # Remove all tokens that have a backslash in them
#     text = re.sub(r'\\', '', text)

#     # If a word has a dash followed by a space at the end, remove the dash and the space
#     text = re.sub(r'-\s+', '', text)

#     # Remove unusual brackets
#     # text = re.sub(r'\[.*?\]', '', text)
#     # text = re.sub(r'\{.*?\}', '', text)

#     # Remove all non-ASCII characters
#     text = re.sub(r'[^\x00-\x7F]+', '', text)

#     # Remove single characters
#     text = re.sub(r'\s+[a-zA-Z]\s+', '', text)

#     # Remove all equation characters like $, \, ^, _, =, etc.
#     text = re.sub(r'\\[a-zA-Z0-9]+', '', text)

#     # Remove all spaces that are not followed by a number or a letter
#     text = re.sub(r'\s+(?=[^a-zA-Z0-9])', '', text)

#     # Remove all spaces that are not preceded by a number or a letter
#     text = re.sub(r'(?<=[^a-zA-Z0-9])\s+', '', text)

#     # Remove all spaces that are not preceded or followed by a number or a letter
#     text = re.sub(r'(?<=[^a-zA-Z0-9])\s+(?=[^a-zA-Z0-9])', '', text)

#     # Remove repeated characters
#     text = re.sub(r'([a-zA-Z0-9])\1{3,}', r'\1', text)

#     # Remove repeated punctuation marks
#     text = re.sub(r'([^\s\w]|_)\1{3,}', r'\1', text)

#     # Insert space after every punctuation mark
#     text = re.sub(r'([^\s\w]|_)+', r'\1 ', text)

#     # Delete equal signs
#     text = re.sub(r'=', '', text)

#     # Insert space before ( and [ if it is not preceded by a space
#     # text = re.sub(r'(?<!\s)(\(|\[)', r' \1', text)

#     # Insert space after ) and ] if it is directly followed by a number or a letter
#     # text = re.sub(r'(\)|\])(?=[a-zA-Z0-9])', r'\1 ', text)

#     # Remove every word that has \ in it
#     text = re.sub(r'\w*\\+\w*', '', text)

#     # Remove single characters that are followed by a ,
#     text = re.sub(r'\s+[a-zA-Z]\s+,', '', text)

#     # Remove all non-english words
#     text = " ".join(w for w in nltk.wordpunct_tokenize(text) if w.lower() in words or not w.isalpha())

#     # Remove all leading and trailing spaces from the text
#     text = text.strip()

#     # Remove punctuation
#     # translator = str.maketrans('', '', string.punctuation)
#     # text = text.translate(translator)

#     return text

# def sorted_files(globber: str):
#     """
#     Give a globbing expression of files to find. They will be sorted upon
#     return.  This function is most useful when sorting does not provide
#     numerical order,

#     e.g.:
#         9 -> 12 returned as 10 11 12 9 by string sort

#     In this case use num_sort=True, and it will be sorted by numbers in the
#     string, then by the string itself.

#     Parameters
#     ----------
#     globber : str
#         Expression on which to search for files (bash glob expression)


#     """
#     files = glob.glob(globber, recursive = True) # return a list of path, including sub directories
#     files.sort()

#     allfiles = []

#     for fn in files:
#         nums = re.findall(r'\d+', fn) # regular expression, find number in path names
#         data = [str(int(n)) for n in nums] + [fn]
#         # a list of [first number, second number,..., filename] in string format otherwise sorted fill fail
#         allfiles.append(data) # list of list

#     allfiles = sorted(allfiles)
#     return [f[-1] for f in allfiles] # sorted filenames

# def reextension(filename: str, extension: str) -> str:
#     """ Give a filename a new extension """
#     name, _ = os.path.splitext(filename)
#     return '{}.{}'.format(name, extension)

# def average_word_length(txt):
#     """
#     Gather statistics about the text, primarily the average word length

#     Parameters
#     ----------
#     txt : str

#     Returns
#     -------
#     word_length : float
#         Average word length in the text
#     """
#     #txt = re.subn(RE_REPEATS, '', txt)[0]
#     nw = len(txt.split())
#     nc = len(txt)
#     avgw = nc / (nw + 1)
#     return avgw


# def process_timeout(cmd, timeout):
#     return check_call(cmd, timeout=timeout, stdout=PIPE, stderr=PIPE)

# # ============================================================================
# #  functions for calling the text extraction services
# # ============================================================================
# def run_pdf2txt(pdffile: str, timelimit: int=TIMELIMIT, options: str=''):
#     """
#     Run pdf2txt to extract full text

#     Parameters
#     ----------
#     pdffile : str
#         Path to PDF file

#     timelimit : int
#         Amount of time to wait for the process to complete

#     Returns
#     -------
#     output : str
#         Full plain text output
#     """
#     # print('Running {} on {}'.format(PDF2TXT, pdffile))
#     tmpfile = reextension(pdffile, 'pdf2txt')

#     cmd = '{cmd} {options} -o "{output}" "{pdf}"'.format(
#         cmd=PDF2TXT, options=options, output=tmpfile, pdf=pdffile
#     )
#     cmd = shlex.split(cmd)
#     output = process_timeout(cmd, timeout=timelimit)

#     with open(tmpfile) as f:
#         return f.read()


# def run_pdftotext(pdffile: str, timelimit: int = TIMELIMIT) -> str:
#     """
#     Run pdftotext on PDF file for extracted plain text

#     Parameters
#     ----------
#     pdffile : str
#         Path to PDF file

#     timelimit : int
#         Amount of time to wait for the process to complete

#     Returns
#     -------
#     output : str
#         Full plain text output
#     """
#     # print('Running {} on {}'.format(PDFTOTEXT, pdffile))
#     tmpfile = reextension(pdffile, 'pdftotxt')

#     cmd = '{cmd} "{pdf}" "{output}"'.format(
#         cmd=PDFTOTEXT, pdf=pdffile, output=tmpfile
#     )
#     cmd = shlex.split(cmd)
#     output = process_timeout(cmd, timeout=timelimit)

#     with open(tmpfile) as f:
#         return f.read()
    
# def run_pdf2txt_A(pdffile: str, **kwargs) -> str:
#     """
#     Run pdf2txt with the -A option which runs 'positional analysis on images'
#     and can return better results when pdf2txt combines many words together.

#     Parameters
#     ----------
#     pdffile : str
#         Path to PDF file

#     kwargs : dict
#         Keyword arguments to :func:`run_pdf2txt`

#     Returns
#     -------
#     output : str
#         Full plain text output
#     """
#     return run_pdf2txt(pdffile, options='-A', **kwargs)

# # ============================================================================
# #  main function which extracts text
# # ============================================================================
# def fulltext(pdffile: str, timelimit: int = TIMELIMIT):
#     """
#     Given a pdf file, extract the unicode text and run through very basic
#     unicode normalization routines. Determine the best extracted text and
#     return as a string.

#     Parameters
#     ----------
#     pdffile : str
#         Path to PDF file from which to extract text

#     timelimit : int
#         Time in seconds to allow the extraction routines to run

#     Returns
#     -------
#     fulltext : str
#         The full plain text of the PDF
#     """
#     if not os.path.isfile(pdffile):
#         raise FileNotFoundError(pdffile)

#     if os.stat(pdffile).st_size == 0:  # file is empty
#         raise RuntimeError('"{}" is an empty file'.format(pdffile))

#     try:
#         output = run_pdftotext(pdffile, timelimit=timelimit)
#         #output = run_pdf2txt(pdffile, timelimit=timelimit)
#     except (TimeoutExpired, CalledProcessError, RuntimeError, OSError) as e:
#         output = run_pdf2txt(pdffile, timelimit=timelimit)
#         #output = run_pdftotext(pdffile, timelimit=timelimit)

#     output = fixunicode.fix_unicode(output)
#     #output = stamp.remove_stamp(output, split=STAMP_SEARCH_LIMIT)
#     wordlength = average_word_length(output)

#     if wordlength <= 45:
#         try:
#             os.remove(reextension(pdffile, 'pdftotxt'))  # remove the tempfile
#         except OSError:
#             pass

#         return output

#     output = run_pdf2txt_A(pdffile, timelimit=timelimit)
#     output = fixunicode.fix_unicode(output)
#     #output = stamp.remove_stamp(output, split=STAMP_SEARCH_LIMIT)
#     wordlength = average_word_length(output)

#     if wordlength > 45:
#         raise RuntimeError(
#             'No accurate text could be extracted from "{}"'.format(pdffile)
#         )

#     try:
#         os.remove(reextension(pdffile, 'pdftotxt'))  # remove the tempfile
#     except OSError:
#         pass

#     return output

# def convert(path: str, skipconverted=True, timelimit: int = TIMELIMIT) -> str:
#     """
#     Convert a single PDF to text.

#     Parameters
#     ----------
#     path : str
#         Location of a PDF file.

#     skipconverted : boolean
#         Skip conversion when there is a text file already

#     Returns
#     -------
#     str
#         Location of text file.
#     """
#     if not os.path.exists(path):
#         raise RuntimeError('No such path: %s' % path)
#     outpath = reextension(path, 'txt')

#     if os.path.exists(outpath):
#         return outpath

#     try:
#         content = fulltext(path, timelimit)
#         with open(outpath, 'w') as f:
#             f.write(content)
#     except Exception as e:
#         msg = "Conversion failed for '%s': %s"
#         print(msg % (path, e))
#         raise RuntimeError(msg % (path, e)) from e
#     return outpath

# def convert_safe(pdffile: str, timelimit: int = TIMELIMIT):
#     """ Conversion function that never fails """
#     try:
#         convert(pdffile, timelimit=timelimit)
#     except Exception as e:
#         print("Conversion failed for '%s': %s" % (pdffile, e))

def process(arxiv_metadata_df, arxiv_storage_size=None):
    try:
        # Convert the PDFs to text and save them to a JSON file

        globber = os.path.join("./tmp/arxiv_pdf", '**/*.pdf') # search expression for glob.glob
        pdffiles = pdf2txt.sorted_files(globber)  # a list of path

        CPU_COUNT = os.cpu_count() - 2
        # CPU_COUNT = 4
        print("Using " + str(CPU_COUNT) + " cores (" + str(os.cpu_count()) + " are available).")
        pool = Pool(CPU_COUNT)
        result = pool.map(partial(pdf2txt.convert_safe, timelimit=TIMELIMIT), pdffiles)
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
            is_processed = process(arxiv_metadata_df)

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