## Sparkify ETL Process

### How to use
1. Create Postgres Tables

```
python create_tables.py
```

2. Run ETL Process

```
python etl.py
```

### 1. Purpose of this DB
The existence of this database will enable the business to better understand their customers. By being able to query for song plays the business will be able to **better understand which users are listening to what songs**. This could form basis for **personalisation** and **recommendations** within the product

### 2. State and justify your 

Database schema design
---
For this DB we have chosen to go with a Star schema design. 

Benefits of this schema:
- Simplify queries for songplays
- Enable faster aggregations

ETL pipeline
---
For this pipeline we have gone with a simple python script which reads in json files, makes some basic transformations in pandas and then writes the formed data to the db

Benefits of this pipeline:
- Simple to implement
- Easy to use
- Suitable for smaller amounts of data
