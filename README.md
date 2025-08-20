Brazil Race Classifier
===========================

[![Python 3.10](https://img.shields.io/badge/python-3.10-blue.svg)](https://www.python.org/downloads/release/python-31011/)
[![MIT License](http://img.shields.io/badge/license-MIT-green.svg)](https://github.com/gabrielsgaspar/brazil-race-classifier/blob/main/LICENSE)
[![Commit](https://img.shields.io/github/last-commit/gabrielsgaspar/brazil-race-classifier)](https://github.com/gabrielsgaspar/brazil-race-classifier/commits/main)

---

## 1. Overview

Auxiliary code for the paper **“Conservation Through Representation: Indigenous Politicians and Forest Protection in the Brazilian Amazon”** by [Gabriel S. Gaspar](https://www.gabrielsgaspar.com/) and [Guy Pincus](https://www.guypincus.com/).

This repository hosts the **Brazil Race Classifier**, a machine learning project that aims to build a classification algorithm for the racial identification of Brazilian politicians.  

The motivation for this work is rooted in political economy research, especially in the context of racial identity, representation, and electoral dynamics in Brazil. The algorithm attempts to classify politicians into racial categories (*indigenous, black, white, pardo*) using available image and textual data.  

## 2. Virtual Environment & Dependencies

This project uses a local **Python virtual environment** (`.venv`) and can be managed via **Poetry**.

### Quickstart

1. **Clone the repository**
   ```bash
   git clone https://github.com/gabrielsgaspar/brazil-race-classifier.git
   cd brazil-race-classifier
   ```

2. **Create and activate the virtual environment**

   - **Windows (PowerShell)**:
     ```powershell
     python -m venv .venv
     .\.venv\Scripts\Activate.ps1
     ```

   - **macOS/Linux**:
     ```bash
     python3 -m venv .venv
     source .venv/bin/activate
     ```

3. **Install dependencies**
    - Install Poetry via pip
    ```bash
    pip install poetry
    ```

    - Install dependencies listed in ``pyproject.toml``
     ```bash
     poetry install
     ```

## 3. Repository Structure

```
brazil-race-classifier/
    ├── .venv/
    ├── .vscode/             
    ├── .gitignore
    ├── README.md
    ├── LICENSE
    ├── pyproject.toml
    └── configs/
       └── tse_urls.yaml
```

## Status

⚠️ **This project is under active development.**  
Features, code structure, and documentation will evolve as the classifier is improved and tested.

---

## License

This project is licensed under the MIT License – see the [LICENSE](https://github.com/gabrielsgaspar/brazil-race-classifier/blob/main/LICENSE) file for details.
