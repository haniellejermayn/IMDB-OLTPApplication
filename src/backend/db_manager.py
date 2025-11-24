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
        
        # Wait for nodes to be ready on startup
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
        """Get titles with pagination"""
        offset = (page - 1) * limit
        conn = self.get_connection('node1')
        
        if not conn:
            return {'error': 'Node 1 unavailable', 'data': []}
        
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
                'limit': limit
            }
        except Error as e:
            logger.error(f"Error fetching titles: {e}")
            return {'error': str(e), 'data': []}
        finally:
            conn.close()
    
    def get_title_by_id(self, tconst):
        """Get single title by ID"""
        conn = self.get_connection('node1')
        
        if not conn:
            return {'error': 'Node 1 unavailable'}
        
        try:
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT * FROM titles WHERE tconst = %s", (tconst,))
            title = cursor.fetchone()
            return title if title else {'error': 'Title not found'}
        except Error as e:
            logger.error(f"Error fetching title: {e}")
            return {'error': str(e)}
        finally:
            conn.close()
    
    def execute_query(self, node_name, query, params=None, isolation_level='READ COMMITTED'):
        """Execute query on specific node"""
        conn = self.get_connection(node_name, isolation_level)
        
        if not conn:
            return {'success': False, 'error': f'{node_name} unavailable'}
        
        try:
            cursor = conn.cursor()
            cursor.execute(query, params or ())
            conn.commit()
            return {'success': True, 'rows_affected': cursor.rowcount}
        except Error as e:
            conn.rollback()
            logger.error(f"Error executing query on {node_name}: {e}")
            return {'success': False, 'error': str(e)}
        finally:
            conn.close()
    
    def get_transaction_logs(self, limit=50):
        """Get recent transaction logs"""
        conn = self.get_connection('node1')
        
        if not conn:
            return {'error': 'Node 1 unavailable', 'logs': []}
        
        try:
            cursor = conn.cursor(dictionary=True)
            cursor.execute("""
                SELECT * FROM transaction_log 
                ORDER BY timestamp DESC 
                LIMIT %s
            """, (limit,))
            logs = cursor.fetchall()
            return {'logs': logs}
        except Error as e:
            logger.error(f"Error fetching logs: {e}")
            return {'error': str(e), 'logs': []}
        finally:
            conn.close()