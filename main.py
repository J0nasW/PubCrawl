###############################################################################################
# 
# Welcome to PubCrawl. This is the main file for the library.
#
###############################################################################################

from kaggle.api.kaggle_api_extended import KaggleApi
import pandas as pd
import streamlit as st
import json, os

### STREAMLIT Init:

# SETTING PAGE CONFIG TO WIDE MODE AND ADDING A TITLE AND FAVICON
st.set_page_config(layout="wide", page_title="PubCrawl", page_icon="📚")

st.title("📚 PubCrawl: A Python library for crawling scientific publications.")

st.markdown("This is a Python library for crawling scientific publications. It is currently in development. Please check back later for updates.")
st.markdown("It is suggested that you run this instance with [tmux](https://github.com/tmux/tmux) because downloads can take a lot of time.")

with st.form(key="pub_crawl_form"):
    a_tab, b_tab, c_tab = st.tabs(["arXiv", "OpenAlex", "PubMed"])

    with a_tab:
        use_arxiv = st.checkbox("arXiv", value=False, key="arxiv_checkbox")

        metadata_procurement = st.selectbox("Metadata Procurement", ["Kaggle (not available yet)", "Local File"])

        st.markdown("### First download the current arXiv Metadata JSON from [Kaggle Datasets](https://www.kaggle.com/Cornell-University/arxiv).")

        st.markdown("### Then provide the file path to the downloaded file below.")
        st.markdown("It is recommended that you place the file in the `datasources` folder.")

        metadata_file = st.text_input("Metadata File Path", value="datasources/arxiv-metadata-oai-snapshot.json")

        st.markdown("### Select a category to filter the arXiv Metadata JSON by.")
        st.markdown("If you do not want to filter the arXiv Metadata JSON, leave this field blank. You can find a list of arXiv categories [here](https://arxiv.org/category_taxonomy).")
        arxiv_category = st.text_input("arXiv Category", placeholder="cs.AI")

        st.markdown("### Select some processing options.")
        st.multiselect("Processing Options", ["Download Fulltexts", "Transformer Keywords for Abstracts", "..."])
    
    with b_tab:
        st.checkbox("OpenAlex", value=False, key="openalex_checkbox")

    with c_tab:
        st.checkbox("PubMed", value=False, key="pubmed_checkbox")

    submit_button = st.form_submit_button(label="Submit")

if submit_button:
    if use_arxiv:
        st.write("You have chosen to analyze arXiv.")
        if metadata_procurement == "Kaggle":
            st.write("You have chosen to download the arXiv Metadata JSON from Kaggle. Please provide your Kaggle API credentials. We will never store your credentials.")
            # with st.form(key="kaggle_credentials_form"):
            #     kaggle_username = st.text_input("Kaggle Username")
            #     kaggle_key = st.text_input("Kaggle Key")
            #     submit_kaggle_credentials_button = st.form_submit_button(label="Submit")
            # if submit_kaggle_credentials_button:
            #     st.markdown("Downloading arXiv Metadata JSON file from Kaggle...")
            #     with st.spinner("Downloading arXiv Metadata JSON file from Kaggle..."):
            #         api_token = {"username":kaggle_username,"key":kaggle_key}
            #         with open("tmp/kaggle.json", "w") as file:
            #             json.dump(api_token, file)
            #         os.system("chmod 600 /tmp/kaggle.json")
            #         os.system("kaggle config path -p /tmp")
            #         os.system("kaggle datasets download -d Cornell-University/arxiv -p tmp")
            #         for file in os.listdir("tmp"):
            #             if file.endswith(".zip"):
            #                 os.system(f"unzip tmp/{file} -d tmp")
            #                 os.system(f"rm tmp/{file}")
            #         arxiv_metadata = api.dataset_download_file("Cornell-University/arxiv", path="tmp/arxiv-metadata-oai-snapshot.json", unzip=True)
            #    st.success("Download complete!")
        
        # Load the file into a Pandas DataFrame
        st.markdown("Loading arXiv Metadata JSON file into a Pandas DataFrame...")
        with st.spinner("Loading arXiv Metadata JSON file into a Pandas DataFrame..."):
            if metadata_procurement == "Kaggle":
                arxiv_metadata = pd.read_json("tmp/arxiv-metadata-oai-snapshot.json")
            elif metadata_procurement == "Local File":
                arxiv_metadata = pd.read_json(arxiv_metadata_file)

        st.success("Load complete!")

        st.write(arxiv_metadata.head())




