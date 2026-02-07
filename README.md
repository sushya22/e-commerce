# E-commerce Data ETL

ETL project for processing Olist datasets. The repo contains readers, an extractor, and a transformers.


Project structure
- `main.py` — small entrypoint used for quick runs
- `workflow.py` — orchestration / scripts
- `src/` — package code
  - `DataSourceReader/` — CSV/Parquet readers
  - `Extractor/` — extraction logic
  - `Transformer/` — transform logic
- `data/` — sample CSV data used by the project

Prerequisites
- Python 3.10+ installed


Setup
1. Create and activate a virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

Run
- Quick run (example):

```bash
python main.py
```


