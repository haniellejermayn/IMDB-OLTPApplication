import mysql.connector
from mysql.connector import Error
import logging
import time

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        self.nodes = {
            'node1': {'host': 'node1-central', 'port': 3306, 'db': 'imdb_distributed'},
            'node2': {'host': 'node2-movies', 'port': 3306, 'db': 'imdb_distributed'},
            'node3': {'host': 'node3-nonmovies', 'port': 3306, 'db': 'imdb_distributed'}
        }
        self.user = 'root'
        self.password = 'password123'
        
        self._wait_for_nodes()
    
    def _wait_for_nodes(self, max_retries=30, delay=2):
        """Wait for all database nodes to be ready"""
        logger.info("Waiting for database nodes to be ready...")
        
        for node_name in self.nodes.keys():
            retries = 0
            while retries < max_retries:
                try:
                    conn = self._create_connection(node_name)
                    if conn:
                        conn.close()
                        logger.info(f"✓ {node_name} is ready")
                        break
                except Exception as e:
                    retries += 1
                    if retries >= max_retries:
                        logger.error(f"✗ {node_name} failed to connect after {max_retries} attempts")
                        raise Exception(f"Could not connect to {node_name}")
                    logger.warning(f"⟳ {node_name} not ready, retrying ({retries}/{max_retries})...")
                    time.sleep(delay)
        
        logger.info("All database nodes are ready!")
    
    def _create_connection(self, node_name):
        """Create raw connection without isolation level setting"""
        node = self.nodes[node_name]
        return mysql.connector.connect(
            host=node['host'],
            port=node['port'],
            database=node['db'],
            user=self.user,
            password=self.password,
            connect_timeout=5
        )
    
    def get_connection(self, node_name, isolation_level='READ COMMITTED', retries=3):
        """Get database connection with specified isolation level and retry logic"""
        last_error = None
        
        for attempt in range(retries):
            try:
                conn = self._create_connection(node_name)
                
                # Set isolation level
                cursor = conn.cursor()
                cursor.execute(f"SET SESSION TRANSACTION ISOLATION LEVEL {isolation_level}")
                cursor.close()
                
                return conn
                
            except Error as e:
                last_error = e
                if attempt < retries - 1:
                    logger.warning(f"Connection attempt {attempt + 1} failed for {node_name}, retrying...")
                    time.sleep(1)
                else:
                    logger.error(f"Error connecting to {node_name} after {retries} attempts: {e}")
        
        return None
    
    def check_node(self, node_name):
        """Check if a specific node is online"""
        conn = self.get_connection(node_name)
        if conn:
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                return True
            except:
                return False
            finally:
                conn.close()
        return False
    
    def check_all_nodes(self):
        """Check health of all nodes"""
        status = {}
        for node_name in self.nodes.keys():
            conn = self.get_connection(node_name)
            if conn:
                try:
                    cursor = conn.cursor(dictionary=True)
                    cursor.execute("SELECT COUNT(*) as count FROM titles")
                    result = cursor.fetchone()
                    status[node_name] = {
                        'status': 'online',
                        'healthy': True,
                        'record_count': result['count']
                    }
                except Error as e:
                    status[node_name] = {
                        'status': 'online',
                        'healthy': False,
                        'error': str(e)
                    }
                finally:
                    conn.close()
            else:
                status[node_name] = {'status': 'offline', 'healthy': False}
        return status
    
    def get_titles(self, page=1, limit=20, title_type=None):
        """Get titles with pagination and automatic fallback"""
        offset = (page - 1) * limit
        conn = self.get_connection('node1')
        
        if conn:
            try:
                cursor = conn.cursor(dictionary=True)
                
                if title_type:
                    query = """
                        SELECT * FROM titles 
                        WHERE title_type = %s 
                        ORDER BY start_year DESC 
                        LIMIT %s OFFSET %s
                    """
                    cursor.execute(query, (title_type, limit, offset))
                else:
                    query = """
                        SELECT * FROM titles 
                        ORDER BY start_year DESC 
                        LIMIT %s OFFSET %s
                    """
                    cursor.execute(query, (limit, offset))
                
                titles = cursor.fetchall()
                
                # Get total count
                cursor.execute("SELECT COUNT(*) as total FROM titles" + 
                            (" WHERE title_type = %s" if title_type else ""),
                            (title_type,) if title_type else ())
                total = cursor.fetchone()['total']
                
                return {
                    'data': titles,
                    'total': total,
                    'page': page,
                    'limit': limit,
                    'source': 'node1'
                }
            except Error as e:
                logger.warning(f"Error fetching titles from node1: {e}")
            finally:
                conn.close()
        
        # fallback
        logger.info(f"Node1 unavailable, using fragment nodes as fallback")
        
        if title_type == 'movie':
            return self._get_titles_from_node('node2', page, limit, title_type)
        elif title_type:
            return self._get_titles_from_node('node3', page, limit, title_type)
        else:
            return self._combine_fragment_titles(page, limit)

    def _get_titles_from_node(self, node_name, page, limit, title_type=None):
        """Helper: Get titles from a specific node"""
        offset = (page - 1) * limit
        conn = self.get_connection(node_name)
        
        if not conn:
            return {
                'error': f'{node_name} unavailable',
                'data': [],
                'total': 0,
                'page': page,
                'limit': limit
            }
        
        try:
            cursor = conn.cursor(dictionary=True)
            
            if title_type:
                query = """
                    SELECT * FROM titles 
                    WHERE title_type = %s 
                    ORDER BY start_year DESC 
                    LIMIT %s OFFSET %s
                """
                cursor.execute(query, (title_type, limit, offset))
            else:
                query = """
                    SELECT * FROM titles 
                    ORDER BY start_year DESC 
                    LIMIT %s OFFSET %s
                """
                cursor.execute(query, (limit, offset))
            
            titles = cursor.fetchall()
            
            cursor.execute("SELECT COUNT(*) as total FROM titles" + 
                        (" WHERE title_type = %s" if title_type else ""),
                        (title_type,) if title_type else ())
            total = cursor.fetchone()['total']
            
            return {
                'data': titles,
                'total': total,
                'page': page,
                'limit': limit,
                'source': node_name
            }
        except Error as e:
            logger.error(f"Error fetching titles from {node_name}: {e}")
            return {
                'error': str(e),
                'data': [],
                'total': 0,
                'page': page,
                'limit': limit
            }
        finally:
            conn.close()

    def _combine_fragment_titles(self, page, limit):
        """Helper: Combine titles from both fragment nodes"""
        offset = (page - 1) * limit
        
        all_titles = []
        total_count = 0
        
        node2_conn = self.get_connection('node2')
        if node2_conn:
            try:
                cursor = node2_conn.cursor(dictionary=True)
                cursor.execute("SELECT * FROM titles ORDER BY start_year DESC")
                all_titles.extend(cursor.fetchall())
                cursor.execute("SELECT COUNT(*) as count FROM titles")
                total_count += cursor.fetchone()['count']
            except Error as e:
                logger.warning(f"Error fetching from node2: {e}")
            finally:
                node2_conn.close()
        
        node3_conn = self.get_connection('node3')
        if node3_conn:
            try:
                cursor = node3_conn.cursor(dictionary=True)
                cursor.execute("SELECT * FROM titles ORDER BY start_year DESC")
                all_titles.extend(cursor.fetchall())
                cursor.execute("SELECT COUNT(*) as count FROM titles")
                total_count += cursor.fetchone()['count']
            except Error as e:
                logger.warning(f"Error fetching from node3: {e}")
            finally:
                node3_conn.close()
        
        if not all_titles:
            return {
                'error': 'All nodes unavailable',
                'data': [],
                'total': 0,
                'page': page,
                'limit': limit
            }
        
        all_titles.sort(key=lambda x: x.get('start_year') or 0, reverse=True)
        paginated_titles = all_titles[offset:offset + limit]
        
        return {
            'data': paginated_titles,
            'total': total_count,
            'page': page,
            'limit': limit,
            'source': 'node2+node3 (combined)'
        }
    
    def search_titles(self, search_term=None, year_from=None, year_to=None, 
                      title_type=None, genre=None, page=1, limit=20):
        """Search titles with filters"""
        offset = (page - 1) * limit
        
        # Build WHERE clause
        conditions = []
        params = []
        
        if search_term:
            conditions.append("primary_title LIKE %s")
            params.append(f"%{search_term}%")
        
        if year_from:
            conditions.append("start_year >= %s")
            params.append(year_from)
        
        if year_to:
            conditions.append("start_year <= %s")
            params.append(year_to)
        
        if title_type:
            conditions.append("title_type = %s")
            params.append(title_type)
        
        if genre:
            conditions.append("genres LIKE %s")
            params.append(f"%{genre}%")
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        # Try node1 first
        conn = self.get_connection('node1')
        
        if conn:
            try:
                cursor = conn.cursor(dictionary=True)
                
                query = f"""
                    SELECT * FROM titles 
                    WHERE {where_clause}
                    ORDER BY start_year DESC 
                    LIMIT %s OFFSET %s
                """
                cursor.execute(query, tuple(params) + (limit, offset))
                titles = cursor.fetchall()
                
                count_query = f"SELECT COUNT(*) as total FROM titles WHERE {where_clause}"
                cursor.execute(count_query, tuple(params))
                total = cursor.fetchone()['total']
                
                return {
                    'data': titles,
                    'total': total,
                    'page': page,
                    'limit': limit,
                    'source': 'node1',
                    'filters': {
                        'search_term': search_term,
                        'year_from': year_from,
                        'year_to': year_to,
                        'title_type': title_type,
                        'genre': genre
                    }
                }
            except Error as e:
                logger.warning(f"Error searching titles from node1: {e}")
            finally:
                conn.close()
        
        # Fallback to appropriate fragment based on title_type filter
        if title_type == 'movie':
            return self._search_from_node('node2', where_clause, params, page, limit)
        elif title_type:
            return self._search_from_node('node3', where_clause, params, page, limit)
        else:
            return self._search_combined_fragments(where_clause, params, page, limit)
    
    def _search_from_node(self, node_name, where_clause, params, page, limit):
        """Helper: Search from a specific node"""
        offset = (page - 1) * limit
        conn = self.get_connection(node_name)
        
        if not conn:
            return {'error': f'{node_name} unavailable', 'data': [], 'total': 0}
        
        try:
            cursor = conn.cursor(dictionary=True)
            
            query = f"""
                SELECT * FROM titles 
                WHERE {where_clause}
                ORDER BY start_year DESC 
                LIMIT %s OFFSET %s
            """
            cursor.execute(query, tuple(params) + (limit, offset))
            titles = cursor.fetchall()
            
            count_query = f"SELECT COUNT(*) as total FROM titles WHERE {where_clause}"
            cursor.execute(count_query, tuple(params))
            total = cursor.fetchone()['total']
            
            return {
                'data': titles,
                'total': total,
                'page': page,
                'limit': limit,
                'source': node_name
            }
        except Error as e:
            return {'error': str(e), 'data': [], 'total': 0}
        finally:
            conn.close()
    
    def _search_combined_fragments(self, where_clause, params, page, limit):
        """Helper: Search and combine from both fragments"""
        offset = (page - 1) * limit
        all_titles = []
        
        for node_name in ['node2', 'node3']:
            conn = self.get_connection(node_name)
            if conn:
                try:
                    cursor = conn.cursor(dictionary=True)
                    query = f"SELECT * FROM titles WHERE {where_clause} ORDER BY start_year DESC"
                    cursor.execute(query, tuple(params))
                    all_titles.extend(cursor.fetchall())
                except Error as e:
                    logger.warning(f"Error searching from {node_name}: {e}")
                finally:
                    conn.close()
        
        all_titles.sort(key=lambda x: x.get('start_year') or 0, reverse=True)
        total = len(all_titles)
        paginated = all_titles[offset:offset + limit]
        
        return {
            'data': paginated,
            'total': total,
            'page': page,
            'limit': limit,
            'source': 'node2+node3 (combined)'
        }
    
    def get_title_by_id(self, tconst):
        """Get single title by ID with automatic fallback"""
        conn = self.get_connection('node1')
        
        if conn:
            try:
                cursor = conn.cursor(dictionary=True)
                cursor.execute("SELECT * FROM titles WHERE tconst = %s", (tconst,))
                title = cursor.fetchone()
                if title:
                    return title
            except Error as e:
                logger.warning(f"Error reading from node1: {e}")
            finally:
                conn.close()
        
        # fallback
        logger.info(f"Node1 unavailable, trying fragment nodes for {tconst}")
        
        for node_name in ['node2', 'node3']:
            conn = self.get_connection(node_name)
            if conn:
                try:
                    cursor = conn.cursor(dictionary=True)
                    cursor.execute("SELECT * FROM titles WHERE tconst = %s", (tconst,))
                    title = cursor.fetchone()
                    if title:
                        logger.info(f"Found {tconst} on {node_name} (fallback)")
                        return title
                except Error as e:
                    logger.warning(f"Error reading from {node_name}: {e}")
                finally:
                    conn.close()
        
        return {'error': 'Title not found in any node'}
    
    def execute_query(self, node_name, query, params=None, isolation_level='READ COMMITTED', autocommit=True):
        conn = self.get_connection(node_name, isolation_level)
        
        if not conn:
            return {'success': False, 'error': f'{node_name} unavailable'}
        
        try:
            if not autocommit:
                conn.start_transaction()
            
            cursor = conn.cursor()
            cursor.execute(query, params or ())
            
            if autocommit:
                conn.commit()
            
            return {'success': True, 'rows_affected': cursor.rowcount, 'connection': conn if not autocommit else None}
        except Error as e:
            if autocommit:
                conn.rollback()
            logger.error(f"Error executing query on {node_name}: {e}")
            return {'success': False, 'error': str(e), 'connection': conn if not autocommit else None}
        finally:
            if autocommit:
                conn.close()
    
    def get_transaction_logs(self, limit=50):
        """Get recent transaction logs"""
        conn = self.get_connection('node1')
        
        if not conn:
            return {'error': 'Node 1 unavailable', 'logs': []}
        
        try:
            cursor = conn.cursor(dictionary=True)
            # FIXED: changed 'timestamp' to 'created_at'
            cursor.execute("""
                SELECT * FROM transaction_log 
                ORDER BY created_at DESC 
                LIMIT %s
            """, (limit,))
            logs = cursor.fetchall()
            return {'logs': logs}
        except Error as e:
            logger.error(f"Error fetching logs: {e}")
            return {'error': str(e), 'logs': []}
        finally:
            conn.close()