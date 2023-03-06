import pandas as pd
import os
import re
import glob
import shlex
from functools import partial
import nltk
nltk.download('words', quiet=True)
words = set(nltk.corpus.words.words())

from subprocess import check_call, CalledProcessError, TimeoutExpired, PIPE

from helpers import fixunicode

TIMELIMIT = 2*60
STAMP_SEARCH_LIMIT = 1000

PDF2TXT = 'pdf2txt.py'
PDFTOTEXT = 'pdftotext'

RE_REPEATS = r'(\(cid:\d+\)|lllll|\.\.\.\.\.|\*\*\*\*\*)'


def cleaned_text(text):
    """
    Clean the text of the PDFs by removing several things
    """
    # Find the first appearance of the word "Introduction" and delete everything before it. Make it case insensitive.
    text = re.sub(r'(?i)^(.*?Introduction)', '', text)
    text = re.sub(r'(?i)^(.*?INTRODUCTION)', '', text)

    # Clean the text
    text = re.sub(r'\s+', ' ', text)

    # Remove every parenthesis, square bracket, and curly bracket
    text = re.sub(r'[\(\)\[\]\{\}]', '', text)

    # Remove special characters
    # text = re.sub(r'[^a-zA-Z0-9\s]', '', text)

    # Remove all numbers from the text
    text = re.sub(r'\d+', '', text)

    # Remove hyperlinks from the text
    text = re.sub(r'https?://\S+', '', text)

    # Remove all tokens that have a backslash in them
    text = re.sub(r'\\', '', text)

    # If a word has a dash followed by a space at the end, remove the dash and the space
    text = re.sub(r'-\s+', '', text)

    # Remove unusual brackets
    # text = re.sub(r'\[.*?\]', '', text)
    # text = re.sub(r'\{.*?\}', '', text)

    # Remove all non-ASCII characters
    text = re.sub(r'[^\x00-\x7F]+', '', text)

    # Remove single characters
    text = re.sub(r'\s+[a-zA-Z]\s+', '', text)

    # Remove all equation characters like $, \, ^, _, =, etc.
    text = re.sub(r'\\[a-zA-Z0-9]+', '', text)

    # Remove all spaces that are not followed by a number or a letter
    text = re.sub(r'\s+(?=[^a-zA-Z0-9])', '', text)

    # Remove all spaces that are not preceded by a number or a letter
    text = re.sub(r'(?<=[^a-zA-Z0-9])\s+', '', text)

    # Remove all spaces that are not preceded or followed by a number or a letter
    text = re.sub(r'(?<=[^a-zA-Z0-9])\s+(?=[^a-zA-Z0-9])', '', text)

    # Remove repeated characters
    text = re.sub(r'([a-zA-Z0-9])\1{3,}', r'\1', text)

    # Remove repeated punctuation marks
    text = re.sub(r'([^\s\w]|_)\1{3,}', r'\1', text)

    # Insert space after every punctuation mark
    text = re.sub(r'([^\s\w]|_)+', r'\1 ', text)

    # Delete equal signs
    text = re.sub(r'=', '', text)

    # Insert space before ( and [ if it is not preceded by a space
    # text = re.sub(r'(?<!\s)(\(|\[)', r' \1', text)

    # Insert space after ) and ] if it is directly followed by a number or a letter
    # text = re.sub(r'(\)|\])(?=[a-zA-Z0-9])', r'\1 ', text)

    # Remove every word that has \ in it
    text = re.sub(r'\w*\\+\w*', '', text)

    # Remove single characters that are followed by a ,
    text = re.sub(r'\s+[a-zA-Z]\s+,', '', text)

    # Remove all non-english words
    text = " ".join(w for w in nltk.wordpunct_tokenize(text) if w.lower() in words or not w.isalpha())

    # Remove all leading and trailing spaces from the text
    text = text.strip()

    # Remove punctuation
    # translator = str.maketrans('', '', string.punctuation)
    # text = text.translate(translator)

    return text

def sorted_files(globber: str):
    """
    Give a globbing expression of files to find. They will be sorted upon
    return.  This function is most useful when sorting does not provide
    numerical order,

    e.g.:
        9 -> 12 returned as 10 11 12 9 by string sort

    In this case use num_sort=True, and it will be sorted by numbers in the
    string, then by the string itself.

    Parameters
    ----------
    globber : str
        Expression on which to search for files (bash glob expression)


    """
    files = glob.glob(globber, recursive = True) # return a list of path, including sub directories
    files.sort()

    allfiles = []

    for fn in files:
        nums = re.findall(r'\d+', fn) # regular expression, find number in path names
        data = [str(int(n)) for n in nums] + [fn]
        # a list of [first number, second number,..., filename] in string format otherwise sorted fill fail
        allfiles.append(data) # list of list

    allfiles = sorted(allfiles)
    return [f[-1] for f in allfiles] # sorted filenames

def reextension(filename: str, extension: str) -> str:
    """ Give a filename a new extension """
    name, _ = os.path.splitext(filename)
    return '{}.{}'.format(name, extension)

def average_word_length(txt):
    """
    Gather statistics about the text, primarily the average word length

    Parameters
    ----------
    txt : str

    Returns
    -------
    word_length : float
        Average word length in the text
    """
    #txt = re.subn(RE_REPEATS, '', txt)[0]
    nw = len(txt.split())
    nc = len(txt)
    avgw = nc / (nw + 1)
    return avgw


def process_timeout(cmd, timeout):
    return check_call(cmd, timeout=timeout, stdout=PIPE, stderr=PIPE)

# ============================================================================
#  functions for calling the text extraction services
# ============================================================================
def run_pdf2txt(pdffile: str, timelimit: int=TIMELIMIT, options: str=''):
    """
    Run pdf2txt to extract full text

    Parameters
    ----------
    pdffile : str
        Path to PDF file

    timelimit : int
        Amount of time to wait for the process to complete

    Returns
    -------
    output : str
        Full plain text output
    """
    # print('Running {} on {}'.format(PDF2TXT, pdffile))
    tmpfile = reextension(pdffile, 'pdf2txt')

    cmd = '{cmd} {options} -o "{output}" "{pdf}"'.format(
        cmd=PDF2TXT, options=options, output=tmpfile, pdf=pdffile
    )
    cmd = shlex.split(cmd)
    output = process_timeout(cmd, timeout=timelimit)

    with open(tmpfile) as f:
        return f.read()


def run_pdftotext(pdffile: str, timelimit: int = TIMELIMIT) -> str:
    """
    Run pdftotext on PDF file for extracted plain text

    Parameters
    ----------
    pdffile : str
        Path to PDF file

    timelimit : int
        Amount of time to wait for the process to complete

    Returns
    -------
    output : str
        Full plain text output
    """
    # print('Running {} on {}'.format(PDFTOTEXT, pdffile))
    tmpfile = reextension(pdffile, 'pdftotxt')

    cmd = '{cmd} "{pdf}" "{output}"'.format(
        cmd=PDFTOTEXT, pdf=pdffile, output=tmpfile
    )
    cmd = shlex.split(cmd)
    output = process_timeout(cmd, timeout=timelimit)

    with open(tmpfile) as f:
        return f.read()
    
def run_pdf2txt_A(pdffile: str, **kwargs) -> str:
    """
    Run pdf2txt with the -A option which runs 'positional analysis on images'
    and can return better results when pdf2txt combines many words together.

    Parameters
    ----------
    pdffile : str
        Path to PDF file

    kwargs : dict
        Keyword arguments to :func:`run_pdf2txt`

    Returns
    -------
    output : str
        Full plain text output
    """
    return run_pdf2txt(pdffile, options='-A', **kwargs)

# ============================================================================
#  main function which extracts text
# ============================================================================
def fulltext(pdffile: str, timelimit: int = TIMELIMIT):
    """
    Given a pdf file, extract the unicode text and run through very basic
    unicode normalization routines. Determine the best extracted text and
    return as a string.

    Parameters
    ----------
    pdffile : str
        Path to PDF file from which to extract text

    timelimit : int
        Time in seconds to allow the extraction routines to run

    Returns
    -------
    fulltext : str
        The full plain text of the PDF
    """
    if not os.path.isfile(pdffile):
        raise FileNotFoundError(pdffile)

    if os.stat(pdffile).st_size == 0:  # file is empty
        raise RuntimeError('"{}" is an empty file'.format(pdffile))

    try:
        output = run_pdftotext(pdffile, timelimit=timelimit)
        #output = run_pdf2txt(pdffile, timelimit=timelimit)
    except (TimeoutExpired, CalledProcessError, RuntimeError, OSError) as e:
        output = run_pdf2txt(pdffile, timelimit=timelimit)
        #output = run_pdftotext(pdffile, timelimit=timelimit)

    output = fixunicode.fix_unicode(output)
    #output = stamp.remove_stamp(output, split=STAMP_SEARCH_LIMIT)
    wordlength = average_word_length(output)

    if wordlength <= 45:
        try:
            os.remove(reextension(pdffile, 'pdftotxt'))  # remove the tempfile
        except OSError:
            pass

        return output

    output = run_pdf2txt_A(pdffile, timelimit=timelimit)
    output = fixunicode.fix_unicode(output)
    #output = stamp.remove_stamp(output, split=STAMP_SEARCH_LIMIT)
    wordlength = average_word_length(output)

    if wordlength > 45:
        raise RuntimeError(
            'No accurate text could be extracted from "{}"'.format(pdffile)
        )

    try:
        os.remove(reextension(pdffile, 'pdftotxt'))  # remove the tempfile
    except OSError:
        pass

    return output

def convert(path: str, skipconverted=True, timelimit: int = TIMELIMIT) -> str:
    """
    Convert a single PDF to text.

    Parameters
    ----------
    path : str
        Location of a PDF file.

    skipconverted : boolean
        Skip conversion when there is a text file already

    Returns
    -------
    str
        Location of text file.
    """
    if not os.path.exists(path):
        raise RuntimeError('No such path: %s' % path)
    outpath = reextension(path, 'txt')

    if os.path.exists(outpath):
        return outpath

    try:
        content = fulltext(path, timelimit)
        with open(outpath, 'w') as f:
            f.write(content)
    except Exception as e:
        msg = "Conversion failed for '%s': %s"
        print(msg % (path, e))
        raise RuntimeError(msg % (path, e)) from e
    return outpath

def convert_safe(pdffile: str, timelimit: int = TIMELIMIT):
    """ Conversion function that never fails """
    try:
        convert(pdffile, timelimit=timelimit)
    except Exception as e:
        print("Conversion failed for '%s': %s" % (pdffile, e))