#!/usr/bin/env python3
import argparse, io, json, nltk, os, re, requests, yaml, zipfile
import pandas as pd
import numpy as np
from collections import Counter
from itertools import tee
from nltk.corpus import stopwords
from google.cloud import storage
from typing import List, Optional
from unidecode import unidecode
from tqdm import tqdm
tqdm.pandas()

# Download stopwords
nltk.download("stopwords")
stopwords_pt = stopwords.words("portuguese")