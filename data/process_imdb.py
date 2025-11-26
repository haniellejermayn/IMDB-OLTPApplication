import csv
import time
import argparse
from datetime import datetime

def process_imdb(generate_node1, generate_node2, generate_node3):
    start_time = time.time()
    print(f"\nStarting data extraction at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    input_file = 'title.basics.tsv'
    output_all = 'node1_all_titles.csv'
    output_movies = 'node2_movies.csv'
    output_nonmovies = 'node3_non_movies.csv'

    count = 0
    type_counts = {'movie': 0, 'non_movie': 0}
    
    files_to_open = {}
    writers = {}
    
    if generate_node1:
        files_to_open['all'] = open(output_all, 'w', newline='', encoding='utf-8')
        writers['all'] = csv.writer(files_to_open['all'], quoting=csv.QUOTE_MINIMAL, escapechar='\\')
        
    if generate_node2:
        files_to_open['movies'] = open(output_movies, 'w', newline='', encoding='utf-8')
        writers['movies'] = csv.writer(files_to_open['movies'], quoting=csv.QUOTE_MINIMAL, escapechar='\\')
        
    if generate_node3:
        files_to_open['nonmovies'] = open(output_nonmovies, 'w', newline='', encoding='utf-8')
        writers['nonmovies'] = csv.writer(files_to_open['nonmovies'], quoting=csv.QUOTE_MINIMAL, escapechar='\\')
    
    header = ['tconst', 'title_type', 'primary_title', 'start_year', 'runtime_minutes', 'genres']
    for writer in writers.values():
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
                count < 40000):  # Stop at 40k rows
                
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
                
                # Clean title: remove quotes, commas, line breaks, null bytes
                title = (row['primaryTitle']
                    .replace('\r', '')
                    .replace('\n', ' ')
                    .replace('\x00', '')
                    .strip())
                
                title_type = row['titleType'].strip()
                tconst = row['tconst'].strip()
                
                row_data = [tconst, title_type, title, year, runtime, genres]
                
                # Write to Node 1 (all)
                if 'all' in writers:
                    writers['all'].writerow(row_data)
                
                # Fragment by title_type
                if title_type == 'movie':
                    if 'movies' in writers:
                        writers['movies'].writerow(row_data)
                    type_counts['movie'] += 1
                else:
                    if 'nonmovies' in writers:
                        writers['nonmovies'].writerow(row_data)
                    type_counts['non_movie'] += 1
                
                count += 1
                
                if count % 10000 == 0:
                    print(f"Processed {count} rows...")
    
    for f in files_to_open.values():
        f.close()
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    print("=" * 60)
    print("DATA EXTRACTION COMPLETE")
    print("=" * 60)
    print(f"Total processed: {count} rows")
    print(f"Processing time: {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)")
    print(f"\nData distribution:")
    print(f"  Movies: {type_counts['movie']} rows ({type_counts['movie']/count*100:.1f}%)")
    print(f"  Non-movies: {type_counts['non_movie']} rows ({type_counts['non_movie']/count*100:.1f}%)")
    print(f"\nVerification: {type_counts['movie']} + {type_counts['non_movie']} = {type_counts['movie'] + type_counts['non_movie']}")
    print(f"Should equal total: {count} ✓" if (type_counts['movie'] + type_counts['non_movie']) == count else f"Should equal total: {count} ✗ ERROR!")
    print("=" * 60)
    print("Files created:")
    if generate_node1:
        print(f"  - {output_all} ({count} rows)")
    if generate_node2:
        print(f"  - {output_movies} ({type_counts['movie']} rows)")
    if generate_node3:
        print(f"  - {output_nonmovies} ({type_counts['non_movie']} rows)")
    print("=" * 60)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process IMDb data for distributed database nodes')
    parser.add_argument('--all', action='store_true', help='Generate all three node files')
    parser.add_argument('--node1', action='store_true', help='Generate Node 1 file (all titles)')
    parser.add_argument('--node2', action='store_true', help='Generate Node 2 file (movies only)')
    parser.add_argument('--node3', action='store_true', help='Generate Node 3 file (non-movies only)')
    
    args = parser.parse_args()
    
    # If --all is set, enable all nodes
    if args.all:
        generate_node1 = True
        generate_node2 = True
        generate_node3 = True
    else:
        # If no flags set, default to all three
        if not (args.node1 or args.node2 or args.node3):
            generate_node1 = True
            generate_node2 = True
            generate_node3 = True
        else:
            generate_node1 = args.node1
            generate_node2 = args.node2
            generate_node3 = args.node3
    
    nodes_list = []
    if generate_node1:
        nodes_list.append('Node 1 (all)')
    if generate_node2:
        nodes_list.append('Node 2 (movies)')
    if generate_node3:
        nodes_list.append('Node 3 (non-movies)')
    
    print(f"\nGenerating files for: {', '.join(nodes_list)}")
    
    process_imdb(generate_node1, generate_node2, generate_node3)