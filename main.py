###############################################################################################
# 
# Welcome to PubCrawl. This is the main file for the library.
#
###############################################################################################

from termcolor import colored
import argparse
import os

from helpers.cli_loader import load_bar
from datasources.arxiv import *
from datasources.pdfs import *



if __name__ == "__main__":

    # Welcome message
    print(colored("Welcome to PubCrawl!", "green", attrs=["bold"]))
    print(colored("--------------------", "green", attrs=["bold"]))
    print("")

    # Instantiate the parser
    parser = argparse.ArgumentParser(description="PubCrawl: A Python library for crawling and cleaning scientific publications.")

    # Add the arguments
    parser.add_argument("-s", "--source", type=str, help="Choose the datasource. Example: -s arxiv", required=True)
    parser.add_argument("-f", "--file", type=str, help="Load the arXiv dataset from a JSON file. Use the Kaggle arXiv Dataset JSON. Example: -f arxiv-metadata-oai-snapshot.json")
    parser.add_argument("-c", "--category", type=str, help='Filter the arXiv dataset by category. Delimit multiple categories AND, OR. Be sure to use quotes. Example: -c "cs.AI AND cs.CL"')
    parser.add_argument("-l", "--local_PDFS", type=str, help="Use local PDFs and provide a PDF dicrectory instead of downloading them. Example: -l ./PDFs")
    parser.add_argument("-p", "--process", action="store_true", help="Process the arXiv dataset: PDF2TXT, Text Cleaning, ... Example: -p")
    parser.add_argument("-g", "--storage_size", type=int, help="Set the maximum storage size for the arXiv dataset download. Lower storage means longer processing. [GB] Example: -s 100")
    parser.add_argument("-r", "--rows", type=int, help="Set the number of rows to be processed. Example: -r 1000")

    # Execute the parse_args() method
    args = parser.parse_args()

    if args.source == "arxiv":

        # Integrity checks
        if not args.file:
            print(colored("Please provide a valid arXiv metdatata JSON file.", "red"))
            os._exit(1)

        if not os.path.isfile(args.file):
            print(colored("The provided file does not exist.", "red"))
            os._exit(1)

        if args.storage_size and not args.process:
            print(colored("You have provided a storage limit but do not wish to process the PDFs. This will result in an error.", "red"))
            os._exit(1)

        if args.local_PDFS and not os.path.isdir(args.local_PDFS):
            print(colored("The provided directory does not exist.", "red"))
            os._exit(1)

        # Print the chosen arguments

        print("")
        print(colored("✓ You have chosen the arXiv Download.", "yellow"))
        print(colored("✓ The file you have provided is valid.", "yellow"))

        if args.category:
            print(colored("✓ You have chosen the category / categories: {}".format(args.category), "yellow"))

        if args.process:
            print(colored("✓ You have chosen to process the data afterwards.", "yellow"))

        if args.storage_size:
            print(colored("✓ You have chosen a storage size of {} GB.".format(args.storage_size), "yellow"))

        print("")
        print(colored("Loading the Metadata...", "green", attrs=["bold"]))
        print("")

        # Load the arXiv JSON and filter for the chosen category
        with load_bar(colored("Filtering arXiv Metadata JSON by category...", "yellow")):
            arxiv_metadata_df = preprocess(arxiv_kaggle_file=args.file, arxiv_category=args.category, arxiv_rows=args.rows)
        
        print("")
        print(colored("Successfully loaded the arXiv metadata and saved it to arxiv_metadata.json.", "green", attrs=["bold"]))
        print("")

        if args.local_PDFS:
            print("")
            print(colored("✓ You have chosen to use local PDFs.", "yellow"))
            print(colored("✓ The directory you have provided is valid.", "yellow"))
            print("")
            print(colored("Loading the PDFs...", "green", attrs=["bold"]))
            print("")

            # Load the local PDFs
            with load_bar(colored("Loading local PDFs...", "yellow")):
                arxiv_metadata_df = process_local_pdfs(arxiv_metadata_df, arxiv_local_PDF_dir=args.local_PDFS)

            print("")
            print(colored("Successfully loaded the local PDFs.", "green", attrs=["bold"]))
            print("")

        # Download the arXiv dataset
        with load_bar(colored("Downloading arXiv PDFs from GCP", "yellow")):
            download_success = download(arxiv_metadata_df, arxiv_storage_size=args.storage_size, arxiv_process=args.process)

        if download_success:

            if args.process:
                print("")
                print(colored("Successfully processed all PDFs to arxiv_fulltext.json", "green", attrs=["bold"]))
                print("")
            else:
                print("")
                print(colored("Successfully downloaded all PDFs to ./tmp/arxiv_pdf", "green", attrs=["bold"]))
                print("")

        else:
            print(colored("An error occured during the download and processing phase.", "red"))
            os._exit(1)

        print(colored("Thank you for using PubCrawl. Good bye!", "green", attrs=["bold"]))
        print(colored("--------------------", "green", attrs=["bold"]))
        print("")

    elif args.source == "pdfs":
        # Integrity checks
        if args.local_PDFS and not os.path.isdir(args.local_PDFS):
            print(colored("The provided directory does not exist.", "red"))
            os._exit(1)

        # Check if there are pdf files in the directory
        if args.local_PDFS:
            pdfs = [f for f in os.listdir(args.local_PDFS) if f.endswith(".pdf")]
            if len(pdfs) == 0:
                print(colored("The provided directory does not contain any PDFs.", "red"))
                os._exit(1)

        if not args.process:
            print(colored("You have not chosen to process the PDFs. This will result in an error. Please add the flag -p.", "red"))
            os._exit(1)

        # Print the chosen arguments
        print("")
        print(colored("✓ You have chosen to process local PDFs.", "yellow"))
        print(colored("✓ The directory you have provided is valid.", "yellow"))
        print(colored("✓ The directory contains valid PDF files.", "yellow"))
        print("")

        # Load the local PDFs
        with load_bar(colored("Loading local PDFs...", "yellow")):
            processed_pdfs = local_pdfs(pdf_dir=args.local_PDFS)

        print("")
        print(colored("Successfully loaded the local PDFs.", "green", attrs=["bold"]))
        print("")




    else:
        print(colored("Please choose a valid datasource.", "red"))
        os._exit(1)

