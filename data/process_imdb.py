import csv
import time
from datetime import datetime

def process_imdb():
    start_time = time.time()
    print(f"\nStarting data extraction at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    input_file = 'title.basics.tsv'
    output_all = 'node1_all_titles.csv'

    count = 0
    
    with open(output_all, 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.writer(outfile, quoting=csv.QUOTE_MINIMAL, escapechar='\\')
        
        header = ['tconst', 'title_type', 'primary_title', 'start_year', 'runtime_minutes', 'genres']
        writer.writerow(header)
        
        print("Reading and processing data...")
        with open(input_file, 'r', encoding='utf-8') as infile:
            reader = csv.DictReader(infile, delimiter='\t')
            
            for row in reader:
                # Filter: non-adult, year >= 2000, valid data
                if (row['isAdult'] == '0' and 
                    row['startYear'] != '\\N' and
                    row['runtimeMinutes'] != '\\N' and 
                    row['primaryTitle'] and
                    int(row['startYear']) >= 2000 and
                    count < 40000):
                    
                    # Clean data
                    year = row['startYear'].strip()
                    runtime = row['runtimeMinutes'].strip()
                    genres = (row['genres']
                             .replace('\\N', 'Unknown')
                             .replace(',', ';')
                             .replace('\r', '')
                             .replace('\n', '')
                             .replace('\x00', '')
                             .strip())
                    
                    title = (row['primaryTitle']
                        .replace('\r', '')
                        .replace('\n', ' ')
                        .replace('\x00', '')
                        .strip())
                    
                    title_type = row['titleType'].strip()
                    tconst = row['tconst'].strip()
                    
                    writer.writerow([tconst, title_type, title, year, runtime, genres])
                    count += 1
                    
                    if count % 10000 == 0:
                        print(f"Processed {count} rows...")
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    print("=" * 60)
    print("DATA EXTRACTION COMPLETE")
    print("=" * 60)
    print(f"Total processed: {count} rows")
    print(f"Processing time: {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)")
    print(f"\nFile created: {output_all}")
    print("=" * 60)
    print("\nNext step: Load into node1, then run initialize_fragments_from_central()")

if __name__ == '__main__':
    process_imdb()