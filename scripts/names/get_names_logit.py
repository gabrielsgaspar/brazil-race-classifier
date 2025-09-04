#!/usr/bin/env python3
import argparse, io, json, nltk, os, re, requests, yaml, zipfile
import pandas as pd
import numpy as np
from nltk.corpus import stopwords
from dotenv import load_dotenv
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from openai import OpenAI
from google.cloud import storage
from typing import List, Optional
from unidecode import unidecode
from tqdm import tqdm
tqdm.pandas()

# Download stopwords
nltk.download("stopwords")
stopwords_pt = stopwords.words("portuguese")


# Function to drop common names from a string
def drop_common_names(text: str, rx: re.Pattern)-> str | None:
    """
    Drops common names from a string using regex
    """
    if pd.isna(text):
        return None
    cleaned = rx.sub("", text)
    cleaned = re.sub(r"\s{2,}", " ", cleaned).strip()
    return cleaned.strip() if cleaned else None


# Main function to train logit and create classification
def main():
    """
    Main function to train logit and create classification
    """

    # Parse command line arguments
    ap = argparse.ArgumentParser(description="Trains a logistic regression model to classify indigenous names and applies it to a list of names using OpenAI API.")
    ap.add_argument("--config", default="configs/cleaning/indigenous_names.yaml", help="Path to YAML with list of names for cleaning")
    ap.add_argument("--prompts", default="prompts/classify_names.yaml", help="Prompt for name scoring classification")
    ap.add_argument("--tse_data_path", default="data/tse/candidates_clean.parquet", help="Path to clean TSE candidates data")
    ap.add_argument("--isa_data_path", default="data/isa/isa_groups.parquet", help="Path to ISA groups data")
    ap.add_argument("--data_years", default=2014, help="The year after which race data exists")
    ap.add_argument("--topk", default=3000, help="Number of top indigenous associated tokens to use in prompt")
    ap.add_argument("--chunk_size", default=500, help="Size of chunks to split the list of names")
    ap.add_argument("--save_path", default="data/names/interim/", help="Path to save the output JSON files")
    ap.add_argument("--states", default="AC AM AP MA MT PA RO RR TO", help="List of states to include in clean data (default Amazon states)")
    args = ap.parse_args()

    # Load names from the configuration file
    with open(args.config, "r") as f:
        config = yaml.safe_load(f) or {}

    # Load prompts from the configuration file
    with open(args.prompts, "r", encoding="utf-8") as f:
        prompts = yaml.safe_load(f) or {}
    
    # Define OpenAI client and system prompt
    load_dotenv()
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    client         = OpenAI(api_key=OPENAI_API_KEY)
    system_message = {"role": "system", "content": (prompts["system"])}
    model          = prompts["model"]["name"]
    temperature    = int(prompts["model"]["temperature"])
    seed           = int(prompts["model"]["seed"])

    # Load relevant dataframes and variables
    df     = pd.read_parquet(args.tse_data_path)
    df_isa = pd.read_parquet(args.isa_data_path)
    amazon_states = args.states.split()
    start_year    = int(args.data_years)
    topk          = int(args.topk)
    chunk_size    = int(args.chunk_size)
    save_path     = args.save_path

    # Organize name variable
    df["candidate_name"] = df["candidate_name"].apply(lambda x: unidecode(x.lower().strip()) if x==x else np.nan)
    df["ballot_name"]    = df["ballot_name"].apply(lambda x: unidecode(x.lower().strip()) if not pd.isna(x) else np.nan)
    df_isa["name"]       = df_isa["Nomes"].apply(lambda x: unidecode(x.lower().strip()) if x==x else np.nan)
    isa_names = df_isa.name.unique().tolist()
    isa_names += [re.sub(r"\s*\(.*?\)\s*", "", i).strip() for i in isa_names]
    isa_names += [m.group(1) for i in isa_names if "(" in i and (m := re.search(r"\((.*?)\)", i))]
    isa_names += config["names"]["isa_names"]
    isa_names = list(set([i for i in isa_names if not "(" in i]))
    
    # Assign indigenous dummy variable by self-declaration and by indigenous name group
    df["indigenous"] = df["race"].apply(lambda x: 1 if x=="indigena" else 0 if x not in ["nao informado", "nao divulgavel"] else np.nan)
    df["indigenous"] = df.apply(lambda x: 1 if any(i in isa_names for i in x.candidate_name.split()) else x.indigenous, axis=1)

    # Find first and last names and other strings to remove
    df["first_name"]  = df["candidate_name"].apply(lambda x: x.split()[0] if x==x else np.nan)
    df["last_name"]   = df["candidate_name"].apply(lambda x: x.split()[-1] if x==x else np.nan)
    first_names       = df["first_name"].unique().tolist()
    common_last_names = df["last_name"].value_counts(normalize=True).reset_index().query("proportion>0.001").last_name.tolist()
    other_names       = config["names"]["exclude_tokens"]
    names_to_drop     = set(first_names + common_last_names + other_names)

    # Define regular expression to drop common names
    rx = re.compile(r"\b(?:" + "|".join(map(re.escape, names_to_drop)) + r")\b")

    # Filter columns, get only amazon states and drop duplicates
    df = df.query("state in @amazon_states", engine="python")
    df = df.drop_duplicates()

    # Clean variables for name
    df["candidate_clean"] = df["candidate_name"].progress_apply(lambda x: drop_common_names(x, rx))
    df["ballot_clean"]    = df["ballot_name"].progress_apply(lambda x: drop_common_names(x, rx))

    # Get dataframe to train Logistic Regression
    df_logit = df.query("year>=2014", engine="python")[["ballot_clean", "indigenous"]].dropna().drop_duplicates().copy()

    # Initiate vectorizer and fit to candidate names for unigrams
    vectorizer = TfidfVectorizer(stop_words=stopwords_pt, ngram_range=(1, 2))
    X = vectorizer.fit_transform(df_logit["ballot_clean"])
    y = df_logit["indigenous"]
    feature_names = vectorizer.get_feature_names_out()

    # Fit logistic regression for unigrams
    lr = LogisticRegression(max_iter=2000, class_weight="balanced", n_jobs=-1)
    lr.fit(X, y)

    # Get coefficient and indices
    coefs    = lr.coef_[0]
    top_inds = coefs.argsort()

    # Retrieve list of top and split list into equal sized chunks
    indig_list        = feature_names[top_inds[-topk:]]
    indig_list_chunks = [indig_list[i:i+chunk_size] for i in range(0, len(indig_list), chunk_size)]

    # Call GPT API in loop to classify names
    for i, name_list in enumerate(indig_list_chunks):
        print(f"Processing chunk {i+1}/{len(indig_list_chunks)} with {len(name_list)} names...")

        # Create user message with list of names
        user_message = {"role": "user", "content": (prompts["user"].format(name_list=name_list))}

        # Define overall message
        messages = [system_message, user_message]

        # Make API call
        response = client.chat.completions.create(model           = model,
                                                  response_format = {"type": "json_object"},
                                                  messages        = messages,
                                                  temperature     = temperature,
                                                  seed            = seed)
        
        # Load ans save results
        res_dict = json.loads(response.choices[0].message.content)
        with open(save_path + f"gpt_name_scores_loop{i+1}.json", "w", encoding="utf-8") as f:
            json.dump(res_dict, f, ensure_ascii=False, indent=2)


# Run script directly
if __name__ == "__main__":
    main()