# Analysis of Child Labor in the Historical Press (Pilot Study)

This repository contains the data, code, and experimental results for the **"Child Labor in the Press"** pilot study. This project demonstrates the approach of the project **aIMPACT: Artificial Intelligence in the Loop of History: Mass Politics and Press Conceptions in Europe and China (1890s–1950s)**, combining expert historical annotation with Large Language Models (LLMs) to analyze child labor discourse across French, German, Chinese, and English-language newspapers (1890–1950).

## 📖 Project Overview

The study tests the feasibility of using few-shot prompting to scale historian-defined interpretive categories across large, multilingual corpora. By leveraging **Gemini**, we propagate expert labels (e.g., *legal framework, labor movement, education, health*) across thousands of articles to uncover temporal regimes of child labor discourse.

### Key Findings

* **France:** Coverage peaked leading up to the 1892 Child Labor Law, followed by a sharp decline.
* **Germany:** Discourse transistions from a necessity to a regulated issue with a peack in pre-wartime years, highlighting the demand for governemnt interventio.
* **China (Chinese & English Press):** A sustained "interwar crescendo" in the 1920s–30s, driven by nationalist reforms, strikes, and international oversight (ILO).
* **overall:**
  1) Strong correlation between discourse and legislation suggesting that the newspapers acted as precursors and catalysts for social and labor legislation;
  2) There is a clear shift towards geovernment responsibility indicating a historical shift from grassroot protest to state-regulated labor protection;
  3) There is a strong alignment between the political orientation of newspapers and topic relevancy (and/or this could indicate newspapertitle-specific engagement);
  4) Labor issues are more relevant in urban contexts: topic prevalence in national newspapers is much higher than in regional, rural-areas newspapers.

---

## 📂 Repository Structure

* `data/annotated_data/`: Manually annotated examples used as few-shot exemplars.
* `data/test_data/`: Unannotated historical corpora.


* `results/`: Annotated test data in JSONL format and statistical summaries and distribution data.


* `./`: Python scripts for API interaction and analysis.

---

## 🚀 Getting Started

### Prerequisites

* Python 3.11+
* A Google Gemini API Key ([Get one here](https://aistudio.google.com/))

### Installation

Clone the repository and install dependencies:

```bash
git clone https://github.com/matejMartinc/analysis_of_child_labor_in_historical_press
cd analysis_of_child_labor_in_historical_press
pip install -r requirements.txt

```

### Usage

#### 1. Corpus Annotation

Set your API key in `gemini_api.py` (line 11) and `gemini_api_de.py` (line 9).

For **Chinese, English, and French** corpora:

```bash
python gemini_api.py

```

For **German** corpora:

```bash
python gemini_api_de.py

```

*Output: Annotated `.jsonl` files stored in the results directory.*

#### 2. Analysis & Visualization

To generate distribution charts and annotation statistics:

```bash
python analyse_annotations.py

```

*Output: PNG charts showing distributions across years, decades, and sources.*

#### 3. Data Conversion

To convert the output files to standard JSON:

```bash
python convert_to_json.py

```

---

## 🛠 Methodology: AI in the loop of historical research

1. **Manual Annotation:** Historians define interpretive categories, searche for, OCRed and manually correct 10 "gold standard" articles per language, and select the wider language corpora (roughly 1000 per language).
2. **Few-Shot Prompting:** These exemplars guide Gemini to apply labels to the wider corpus.
3. **Scaling:** The LLM processes thousands of articles, providing a scale difficult for manual reading.
4. **Manual Verification:** Historians manually verified the validity of the automated annotation in ca. 25 cases per language.
5. **Validation:** Results are cross-referenced with historical events (e.g., France’s 1892 law, China’s May 30th Movement, Child labor legislation in Austria 1859) to ensure analytical validity.

---

## 📜 License

MIT

---
