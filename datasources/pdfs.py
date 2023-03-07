###############################################################################################################################
# 
# Welcome to the local PDF datasource. This file contains the functions for analyzing local PDFs.
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

def local_pdfs(pdf_dir):
    try:
        # Create a folder for the txt files
        if not os.path.exists("./txt"):
            os.makedirs("./txt")  

        # Print the number of pdf files in the folder
        print("Found " + str(len(os.listdir(pdf_dir))) + " PDF files in the folder.")

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

        print("Conversion finished. Now moving the files to the txt folder and deleting the PDF files.")

        # Move the TXT files to the arxiv_txt folder
        for pdffile in pdffiles:
            txtfile = pdf2txt.reextension(pdffile, 'txt')
            shutil.move(txtfile, "./txt")
            #os.remove(pdffile)

        print("Finished moving the files to the txt folder and deleting the PDF files.")
        
        # Open all *.txt files from the fulltext folder and save them in a dataframe
        files = [f for f in os.listdir("./txt") if f.endswith(".txt")]

        print("Finished reading the fulltext files.")

        # Store files in a pandas dataframe - name as id and content as text
        file_list = []
        for file in files:
            with open("./txt/" + str(file), "r") as f:
                text = pdf2txt.cleaned_text(f.read())
                file_list.append([str(file).replace(".txt", ""), str(text)])
                #arxiv_fulltext_df = arxiv_fulltext_df.append({"id": str(file).str.replace("v\d+", "").str.replace(".txt", ""), "text": str(text)}, ignore_index=True)
        pdf_fulltext_df = pd.DataFrame(file_list, columns=["id", "text"])

        print("Finished storing the fulltext files into a dataframe.")

        pdf_fulltext_df.to_json("./pdf_fulltext.json", orient="records", lines=True)

        print("Finished saving the processed PDFs to a JSON file.")

        # Delete the txt folder
        shutil.rmtree("./txt")

        return True
    
    except Exception as e:
        print("Processing failed: " + str(e))
        return False