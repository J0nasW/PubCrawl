# PubCrawl
PubCrawl - The scientific publication crawling suite.

# Overview
PubCrawl is a Python package for crawling scientific publications.
The idea is to provide a simple interface to crawl scientific publications from different sources. The package is designed to be easily extensible to support new sources.

### Current Status
The package is currently in a very early stage of development. The following sources are currently supported:
- arxiv

# Installation
To start, clone the repository and install the package using the requirements file:

```bash
git clone
cd pubcrawl

pip install -r requirements.txt
```

# Usage
The package is designed to be used as a library. The following example shows how to use the package to crawl publications from arxiv.

### Example: Crawling publications from arxiv
The following example shows how to crawl publications from arXiv. The example crawls all publications from the category `cs.AI`, processes them and saves the results to a JSON file. Note, that you have to provide a valid JSON file containing the metadata of all arXiv publications. You can download this file from the Kaggle dataset [here](https://www.kaggle.com/Cornell-University/arxiv).

```bash
python3 main.py --s arxiv -c cs.AI -f arxiv.json -p
```
### API
The package can be called using the following arguments:

| Argument | Type | Description |
| --- | --- | --- |
| -s, --source | required | The source to crawl publications from. |
| -f, --file | required | The file containing the metadata of all publications. |
| -c, --category | optional | The category to crawl publications from. |
| -p, --process | optional | Whether to process the crawled publications. |
| -r, --rows | optional | Number of entries to process (can be useful in development mode) |

# License
Copyright (c) 2023, Jonas Wilinski