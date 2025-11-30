import logging
from db_manager import DatabaseManager
import os

logger = logging.getLogger(__name__)

def initialize_fragments_from_central(db_manager):
    """
    Copy data from central node to fragments, preserving timestamps.
    Run this ONCE on startup after central is populated.
    """
    logger.info("Starting fragment initialization from central node...")
    
    central_node = 'node1'
    
    # Get all movies from central
    conn = db_manager.get_connection(central_node)
    if not conn:
        logger.error("Cannot connect to central node for initialization")
        return False
    
    try:
        cursor = conn.cursor(dictionary=True)
        
        # === MOVIES to node2 ===
        cursor.execute("""
            SELECT tconst, title_type, primary_title, start_year, 
                   runtime_minutes, genres, last_updated
            FROM titles 
            WHERE title_type = 'movie'
        """)
        movies = cursor.fetchall()
        
        logger.info(f"Copying {len(movies)} movies to node2...")
        
        node2_conn = db_manager.get_connection('node2')
        if node2_conn:
            node2_cursor = node2_conn.cursor()
            
            # Clear existing data first
            node2_cursor.execute("DELETE FROM titles")
            
            # Insert with preserved timestamps
            insert_query = """
                INSERT INTO titles 
                (tconst, title_type, primary_title, start_year, runtime_minutes, genres, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            
            for movie in movies:
                node2_cursor.execute(insert_query, (
                    movie['tconst'],
                    movie['title_type'],
                    movie['primary_title'],
                    movie['start_year'],
                    movie['runtime_minutes'],
                    movie['genres'],
                    movie['last_updated'] 
                ))
            
            node2_conn.commit()
            node2_conn.close()
            logger.info(f"✓ Successfully copied {len(movies)} movies to node2")
        
        # === NON-MOVIES to node3 ===
        cursor.execute("""
            SELECT tconst, title_type, primary_title, start_year, 
                   runtime_minutes, genres, last_updated
            FROM titles 
            WHERE title_type != 'movie'
        """)
        non_movies = cursor.fetchall()
        
        logger.info(f"Copying {len(non_movies)} non-movies to node3...")
        
        node3_conn = db_manager.get_connection('node3')
        if node3_conn:
            node3_cursor = node3_conn.cursor()
            node3_cursor.execute("DELETE FROM titles")
            
            for title in non_movies:
                node3_cursor.execute(insert_query, (
                    title['tconst'],
                    title['title_type'],
                    title['primary_title'],
                    title['start_year'],
                    title['runtime_minutes'],
                    title['genres'],
                    title['last_updated']
                ))
            
            node3_conn.commit()
            node3_conn.close()
            logger.info(f"✓ Successfully copied {len(non_movies)} non-movies to node3")
        
        logger.info("✓ Fragment initialization complete!")
        return True
        
    except Exception as e:
        logger.error(f"Error during fragment initialization: {e}")
        return False
    finally:
        conn.close()


def clear_all_nodes(db_manager):
    """
    Delete all data from all nodes.
    Returns dict with results per node.
    """
    logger.info("Clearing all tables...")
    results = []
    
    for node_name in ['node1', 'node2', 'node3']:
        conn = db_manager.get_connection(node_name)
        if conn:
            try:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM titles")
                rows_deleted = cursor.rowcount
                conn.commit()
                results.append({
                    'node': node_name,
                    'success': True,
                    'rows_deleted': rows_deleted
                })
                logger.info(f"  ✓ {node_name}: Deleted {rows_deleted} rows")
            except Exception as e:
                logger.error(f"  ✗ {node_name}: Error - {e}")
                results.append({
                    'node': node_name,
                    'success': False,
                    'error': str(e)
                })
            finally:
                conn.close()
        else:
            results.append({
                'node': node_name,
                'success': False,
                'error': 'Cannot connect to node'
            })
    
    return results


def import_csv_to_node1(db_manager):
    """
    Import CSV file into node1 using LOAD DATA INFILE.
    
    CSV must be located at: /var/lib/mysql-files/node1_all_titles.csv
    
    Setup:
    - Docker: Automatically mounted via docker-compose.yml
    - Cloud VM: Run `sudo cp data/node1_all_titles.csv /var/lib/mysql-files/`
    """
    csv_path = '/var/lib/mysql-files/node1_all_titles.csv'
    
    logger.info(f"Importing CSV into node1 from: {csv_path}")
    
    conn = db_manager.get_connection('node1')
    if not conn:
        error_msg = "Cannot connect to node1"
        logger.error(f"  ✗ {error_msg}")
        return {
            'success': False,
            'error': error_msg
        }
    
    try:
        cursor = conn.cursor()
        
        # LOAD DATA INFILE
        load_query = """
            LOAD DATA INFILE %s
            INTO TABLE titles
            FIELDS TERMINATED BY ',' 
            ENCLOSED BY '"'
            LINES TERMINATED BY '\n'
            IGNORE 1 ROWS
            (tconst, title_type, primary_title, start_year, runtime_minutes, genres)
        """
        cursor.execute(load_query, (csv_path,))
        conn.commit()
        
        # Verify import
        cursor.execute("SELECT COUNT(*) as count FROM titles")
        import_count = cursor.fetchone()[0]
        
        logger.info(f"  ✓ Imported {import_count} rows into node1")
        
        return {
            'success': True,
            'rows_imported': import_count
        }
        
    except Exception as e:
        error_msg = str(e)
        logger.error(f"  ✗ CSV import error: {e}")
        
        if 'cannot find' in error_msg.lower() or 'no such file' in error_msg.lower():
            logger.error(f"  File not found: {csv_path}")
            logger.error(f"  Docker: Check docker-compose.yml volume mount")
            logger.error(f"  Cloud VM: Run `sudo cp data/node1_all_titles.csv /var/lib/mysql-files/`")
        
        return {
            'success': False,
            'error': error_msg
        }
    finally:
        conn.close()


def get_node_counts(db_manager):
    """
    Get row counts from all nodes.
    Returns dict with counts per node.
    """
    counts = {}
    
    for node_name in ['node1', 'node2', 'node3']:
        conn = db_manager.get_connection(node_name)
        if conn:
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) as count FROM titles")
                counts[node_name] = cursor.fetchone()[0]
            except Exception as e:
                counts[node_name] = f"Error: {e}"
            finally:
                conn.close()
        else:
            counts[node_name] = "Offline"
    
    return counts


def reset_and_reinitialize_database(db_manager):
    """
    Complete database reset pipeline:
    1. Clear all nodes
    2. Import CSV to node1
    3. Initialize fragments
    
    Returns detailed results dict.
    """
    logger.info("=" * 60)
    logger.info("STARTING DATABASE RESET PIPELINE")
    logger.info("=" * 60)
    
    results = {
        'steps': [],
        'success': False
    }
    
    logger.info("Step 1: Clearing all tables...")
    clear_results = clear_all_nodes(db_manager)
    
    if not all(r['success'] for r in clear_results):
        results['steps'].append({
            'step': 1,
            'action': 'clear_all_tables',
            'success': False,
            'nodes': clear_results
        })
        return results
    
    results['steps'].append({
        'step': 1,
        'action': 'clear_all_tables',
        'success': True,
        'nodes_cleared': clear_results
    })
    
    logger.info("Step 2: Re-importing CSV into node1...")
    import_result = import_csv_to_node1(db_manager)
    
    if not import_result['success']:
        results['steps'].append({
            'step': 2,
            'action': 'import_csv',
            'success': False,
            'error': import_result.get('error')
        })
        return results
    
    results['steps'].append({
        'step': 2,
        'action': 'import_csv',
        'success': True,
        'rows_imported': import_result['rows_imported']
    })
    
    logger.info("Step 3: Initializing fragments from central...")
    fragment_success = initialize_fragments_from_central(db_manager)
    
    if not fragment_success:
        results['steps'].append({
            'step': 3,
            'action': 'initialize_fragments',
            'success': False,
            'error': 'Fragment initialization failed'
        })
        return results
    
    # Get final counts
    final_counts = get_node_counts(db_manager)
    
    results['steps'].append({
        'step': 3,
        'action': 'initialize_fragments',
        'success': True,
        'node_counts': final_counts
    })
    
    # SUCCESS
    results['success'] = True
    results['message'] = 'Database reset and reinitialization complete!'
    results['summary'] = final_counts
    
    logger.info("=" * 60)
    logger.info("✓ DATABASE RESET COMPLETE")
    for node, count in final_counts.items():
        logger.info(f"  {node}: {count} rows")
    logger.info("=" * 60)
    
    return results