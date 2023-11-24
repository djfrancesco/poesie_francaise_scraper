# poesie_francaise_scraper

- Install the requirements:

```bash
$ pip install requirements.txt
```

- Run the scraper with:

```bash
$ python scraper.py
```

or

```python 
scraper = Scraper(duckdb_file_path=None)
scraper.fetch_all()
```

If `duckdb_file_path` is `None`, a file named `poesie_francaise.duckdb` is created in the current working directory.

The DuckDB database has two tables:
- `poets`
- `poems`

```sql
D SELECT table_catalog, table_schema, table_name, table_type FROM INFORMATION_SCHEMA.TABLES;
```
	┌──────────────────┬──────────────┬────────────┬────────────┐
	│  table_catalog   │ table_schema │ table_name │ table_type │
	│     varchar      │   varchar    │  varchar   │  varchar   │
	├──────────────────┼──────────────┼────────────┼────────────┤
	│ poesie_francaise │ main         │ poems      │ BASE TABLE │
	│ poesie_francaise │ main         │ poets      │ BASE TABLE │
	└──────────────────┴──────────────┴────────────┴────────────┘

```sql
D SELECT COUNT(*) FROM poets;
```
	┌──────────────┐
	│ count_star() │
	│    int64     │
	├──────────────┤
	│           72 │
	└──────────────┘

```sql
D SELECT COUNT(*) FROM poems;
```
	┌──────────────┐
	│ count_star() │
	│    int64     │
	├──────────────┤
	│         5873 │
	└──────────────┘

```sql
D SELECT table_name, column_name FROM INFORMATION_SCHEMA.COLUMNS;
```
	┌────────────┬─────────────┐
	│ table_name │ column_name │
	│  varchar   │   varchar   │
	├────────────┼─────────────┤
	│ poems      │ poet_slug   │
	│ poems      │ poem_title  │
	│ poems      │ poem_slug   │
	│ poems      │ poet_name   │
	│ poems      │ poem_book   │
	│ poems      │ poem_text   │
	│ poets      │ poet_slug   │
	│ poets      │ poet_name   │
	│ poets      │ poet_dob    │
	│ poets      │ poet_dod    │
	├────────────┴─────────────┤
	│ 10 rows        2 columns │
	└──────────────────────────┘
