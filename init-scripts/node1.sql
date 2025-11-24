SOURCE /docker-entrypoint-initdb.d/create_schema.sql;

LOAD DATA LOCAL INFILE '/var/lib/mysql-files/node1_all_titles.csv'
INTO TABLE titles
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(tconst, title_type, primary_title, start_year, runtime_minutes, genres);

SELECT 'Node 1 (Central) - Loaded all titles' AS status, COUNT(*) AS total_rows FROM titles;