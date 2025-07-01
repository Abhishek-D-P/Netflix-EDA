
---

## 🛠️ Data Pipeline Workflow

### 1. Bronze Layer – `clean.py`
- Reads raw data (`netflix.csv`)
- Drops unnecessary columns (e.g., `show_id`)
- Imputes missing values
- Saves cleaned data to `data/bronze.csv`

### 2. Silver Layer – `transform.py`
- Splits multi-valued columns: `director`, `cast`, `country`, `listed_in`
- Parses `date_added` to derive `year_added`, `month_added`, `day_of_week`
- Saves transformed data to `data/silver.csv`

### 3. Gold Layer – `feature_engg.py`
- Extracts `Movie_duration` or `Seasons` based on type
- Drops redundant columns like `duration`
- Saves final data to `data/gold.csv`

---

## 📊 Tableau Dashboard Features

Built using the `data/gold.csv` file, the dashboard includes:

- ✅ **Total count of Movies and TV Shows**
- 📈 **Line plot of movies released over time**
- 🌍 **Map showing movie countries**
- 🌲 **Treemap of ratings**
- ⏱️ **Average movie duration**
- 🗂️ **Interactive movie table**
- 📅 **Bar plots: Releases by weekday and month**

### 🔍 Global Filters:
- 🎭 Cast
- 🎬 Director
- 🎞️ Genre (listed_in)
- 📅 Date Added
- ⭐ Rating

---

## 📝 How to Run the Pipeline

```bash
# Run entire pipeline to generate final 'gold' dataset
python feature_engg.py
