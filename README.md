# PubCrawl
PubCrawl - The scientific publication crawling suite.

### Overview
PubCrawl is a Python package for crawling scientific publications.
The idea is to provide a simple interface to crawl scientific publications from different sources. The package is designed to be easily extensible to support new sources.

### Current Status
The package is currently in a very early stage of development. The following sources are currently supported:
- [arXiv](https://arxiv.org/)
- pdfs - Plain PDF Files

## Installation
To start, clone the repository and install the package using the requirements file:

```bash
git clone https://github.com/J0nasW/PubCrawl.git
cd pubcrawl

pip install -r requirements.txt
```

## Usage
The package is designed to be used as a library. The following example shows how to use the package to crawl publications from arXiv.

### Example: Crawling publications from arXiv
The following example shows how to crawl publications from arXiv. The example crawls all publications from the category `cs.AI`, processes them and saves the results to a JSON file. Note, that you have to provide a valid JSON file containing the metadata of all arXiv publications. You can download this file from the Kaggle dataset [here](https://www.kaggle.com/Cornell-University/arxiv).

```bash
python3 main.py --s arxiv -c cs.AI -f arxiv.json -p
```
### API
The package can be called using the following arguments:

| Argument | Type | Description |
| --- | --- | --- |
| -s, --source | required | The source to crawl publications from. |
| -f, --file | optional | The file containing the metadata of all publications. |
| -l, --local_PDFS | optional | Use local PDFs and provide a PDF dicrectory instead of downloading them from GCP. |
| -c, --category | optional | The category to crawl publications from. Delimit multiple categories with a | for OR and & for AND. Example: "cs.AI AND cs.CL" |
| -p, --process | optional | Whether to process the crawled publications. |
| -r, --rows | optional | Number of entries to process (can be useful in development mode) |

# Downstream Tasks
This package can be used in combination with other packages to perform downstream tasks. The following packages are currently available:
- [PubGraph](https://github.com/J0nasW/PubGraph)

When cloned the repository, you can use the extra flag **-d** to activate downstream tasks. PubCrawl will then automatically clone the downstream task repository and install the package using the requirements file.

The following example shows how to crawl all publications from the category `cs.AI` and create a graph from them.

```bash
python3 main.py --s arxiv -c cs.AI -f arxiv.json -p -d pubgraph
```

### Using PubGraph to create publication graphs
tbd

# License & Contributions
Copyright (c) 2023, Jonas Wilinski
A lot of code regarding the PDF2TXT conversion is taken from [this repository](https://github.com/mattbierbaum/arxiv-public-datasets), which is licensed under the MIT license. Thank you, Matt Bierbaum, Colin Clement, Kevin O'Keeffe, Alex Alemi for sharing your code!