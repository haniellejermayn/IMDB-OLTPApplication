import logging
from db_manager import DatabaseManager

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