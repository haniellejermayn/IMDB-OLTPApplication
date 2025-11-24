LOAD DATA INFILE '/var/lib/mysql-files/node3_non_movies.csv'
INTO TABLE titles
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(tconst, title_type, primary_title, start_year, runtime_minutes, genres);

SELECT 'Node 3 (Non-Movies) - Loaded non-movie data' AS status, COUNT(*) AS total_rows FROM titles;