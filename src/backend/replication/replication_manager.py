import logging
import json
from .transaction_logger import TransactionLogger
from .recovery_handler import RecoveryHandler
from .concurrency_tester import ConcurrencyTester
import threading

logger = logging.getLogger(__name__)

class ReplicationManager:
    def __init__(self, db_manager):
        self.db = db_manager
        self.transaction_logger = TransactionLogger(db_manager)
        self.recovery_handler = RecoveryHandler(db_manager, self.transaction_logger)
        self.concurrency_tester = ConcurrencyTester(db_manager, self)
        self._id_lock = threading.Lock()
    
    def _get_primary_node(self, title_type):
        return 'node2' if title_type == 'movie' else 'node3'

    def _get_new_tconst_transactional(self, conn):
        """
        Generate new tconst within an existing transaction.
        This prevents race conditions by holding the lock until insert completes.
        
        Args:
            conn: Active database connection with transaction started
        
        Returns:
            str: New tconst like 'tt0001234'
        """
        try:
            cursor = conn.cursor(dictionary=True)
            
            # Lock the table and get MAX - lock held until transaction commits
            cursor.execute("SELECT MAX(tconst) as max_id FROM titles FOR UPDATE")
            result = cursor.fetchone()
            max_tconst = result['max_id']
            
            if max_tconst is None:
                new_tconst = 'tt0000001'
            else:
                numeric_part = int(max_tconst[2:])
                new_id = numeric_part + 1
                new_tconst = f'tt{new_id:07d}'
            
            logger.info(f"Generated new tconst: {new_tconst} (max was: {max_tconst})")
            return new_tconst
            
        except Exception as e:
            logger.error(f"Error generating tconst: {e}")
            raise Exception(f"Failed to generate new tconst: {e}")

    def insert_title(self, data):
        """
        IMPROVED Insert flow with atomic ID generation:
        
        Strategy:
        1. Check if primary fragment is available
        2. If YES:
        - Generate ID from central in separate transaction
        - Insert to primary fragment
        - Replicate to central
        3. If NO (primary fragment down):
        - Generate ID and insert to central in SAME transaction (atomic!)
        - Queue replication to fragment
        
        This prevents:
        - ID collisions
        - Orphaned IDs (generated but not used)
        - Race conditions
        """
        title_type = data.get('title_type')
        primary_node = self._get_primary_node(title_type)
        central_node = 'node1'
        
        primary_available = self.db.check_node(primary_node)
        
        if primary_available:
            # === CASE A: Primary fragment is UP ===
            try:
                tconst = self._get_new_tconst()
            except Exception as e:
                return {
                    'success': False,
                    'error': f'Failed to generate tconst: {str(e)}',
                    'message': 'Cannot proceed without valid ID'
                }
            
            # Insert to primary fragment (MySQL sets last_updated automatically)
            query = """
                INSERT INTO titles (tconst, title_type, primary_title, start_year, runtime_minutes, genres)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            params = (
                tconst,
                title_type,
                data.get('primary_title'),
                data.get('start_year'),
                data.get('runtime_minutes'),
                data.get('genres')
            )
            
            results = {}
            result_primary = self.db.execute_query(primary_node, query, params)
            results[primary_node] = result_primary
            
            if result_primary['success']:
                logger.info(f"✓ INSERT to PRIMARY {primary_node} succeeded for {tconst}")
                
                # Fetch the record with its timestamp
                record = self.db.get_title_by_id(tconst)
                if 'error' in record:
                    logger.error(f"✗ Failed to fetch inserted record {tconst}")
                    return {
                        'success': False,
                        'error': 'Insert succeeded but failed to fetch record for replication'
                    }
                
                last_updated = record.get('last_updated')
                
                # Replicate to central WITH explicit timestamp
                replication_query = """
                    INSERT INTO titles 
                    (tconst, title_type, primary_title, start_year, runtime_minutes, genres, last_updated)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                replication_params = params + (last_updated,)
                
                result_central = self.db.execute_query(central_node, replication_query, replication_params)
                results[central_node] = result_central
                
                if result_central['success']:
                    self.transaction_logger.log_replication(
                        source_node=primary_node,
                        target_node=central_node,
                        operation_type='INSERT',
                        record_id=tconst,
                        query=replication_query,  # Log the replication query, not the original
                        params=replication_params,
                        status='SUCCESS'
                    )
                    logger.info(f"✓ INSERT replicated to CENTRAL for {tconst}")
                    
                    return {
                        'success': True,
                        'tconst': tconst,
                        'primary_node': primary_node,
                        'replicated_to': central_node,
                        'results': results,
                        'message': f'Insert committed to {primary_node} and replicated to {central_node}'
                    }
                else:
                    # Central replication failed - queue for retry
                    transaction_id = self.transaction_logger.log_replication(
                        source_node=primary_node,
                        target_node=central_node,
                        operation_type='INSERT',
                        record_id=tconst,
                        query=replication_query,
                        params=replication_params,
                        status='PENDING',
                        error_msg=result_central.get('error')
                    )
                    
                    logger.warning(f"⚠ REPLICATION FAILED: {primary_node} → {central_node} for {tconst}. Queued.")
                    
                    return {
                        'success': True,
                        'tconst': tconst,
                        'primary_node': primary_node,
                        'replicated_to': None,
                        'pending_replication': central_node,
                        'transaction_id': transaction_id,
                        'results': results,
                        'message': f'Insert committed to {primary_node}. Replication to {central_node} queued.'
                    }
            else:
                # Primary insert failed - try central as fallback
                logger.error(f"✗ INSERT to PRIMARY {primary_node} failed for {tconst}: {result_primary.get('error')}")
                logger.warning(f"⚠ Attempting central as fallback for {tconst}")
                
                result_central = self.db.execute_query(central_node, query, params)
                results[central_node] = result_central
                
                if result_central['success']:
                    # Fetch record from central to get timestamp
                    record = self.db.get_title_by_id(tconst)
                    last_updated = record.get('last_updated')
                    
                    replication_query = """
                        INSERT INTO titles 
                        (tconst, title_type, primary_title, start_year, runtime_minutes, genres, last_updated)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """
                    replication_params = params + (last_updated,)
                    
                    transaction_id = self.transaction_logger.log_replication(
                        source_node=central_node,
                        target_node=primary_node,
                        operation_type='INSERT',
                        record_id=tconst,
                        query=replication_query,
                        params=replication_params,
                        status='PENDING',
                        error_msg=f'{primary_node} insert failed: {result_primary.get("error")}'
                    )
                    
                    return {
                        'success': True,
                        'tconst': tconst,
                        'primary_node': central_node,
                        'replicated_to': None,
                        'pending_replication': primary_node,
                        'transaction_id': transaction_id,
                        'results': results,
                        'message': f'Insert committed to {central_node} (fallback). Queued for {primary_node}.'
                    }
                else:
                    return {
                        'success': False,
                        'error': 'All target nodes failed',
                        'results': results,
                        'message': f'Insert failed on both {primary_node} and {central_node}'
                    }
        
        else:
            # === CASE B: Primary fragment is DOWN - atomic insert to central ===
            logger.warning(f"⚠ PRIMARY {primary_node} unavailable, using central with atomic ID generation")
            
            conn = self.db.get_connection(central_node, 'SERIALIZABLE')
            
            if not conn:
                return {
                    'success': False,
                    'error': f'Both {primary_node} and {central_node} unavailable',
                    'message': 'Cannot perform insert - all nodes down'
                }
            
            try:
                conn.start_transaction()
                tconst = self._get_new_tconst_transactional(conn)
                
                query = """
                    INSERT INTO titles (tconst, title_type, primary_title, start_year, runtime_minutes, genres)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """
                params = (
                    tconst,
                    title_type,
                    data.get('primary_title'),
                    data.get('start_year'),
                    data.get('runtime_minutes'),
                    data.get('genres')
                )
                
                cursor = conn.cursor(dictionary=True)
                cursor.execute(query, params)
                
                # Fetch the timestamp that was just created
                cursor.execute("SELECT last_updated FROM titles WHERE tconst = %s", (tconst,))
                result_row = cursor.fetchone()
                last_updated = result_row['last_updated']
                
                conn.commit()
                
                logger.info(f"✓ ATOMIC INSERT to CENTRAL (fallback) succeeded for {tconst}")
                
                # Queue replication WITH timestamp
                replication_query = """
                    INSERT INTO titles 
                    (tconst, title_type, primary_title, start_year, runtime_minutes, genres, last_updated)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                replication_params = params + (last_updated,)
                
                transaction_id = self.transaction_logger.log_replication(
                    source_node=central_node,
                    target_node=primary_node,
                    operation_type='INSERT',
                    record_id=tconst,
                    query=replication_query,
                    params=replication_params,
                    status='PENDING',
                    error_msg=f'{primary_node} was unavailable during insert'
                )
                
                return {
                    'success': True,
                    'tconst': tconst,
                    'primary_node': central_node,
                    'replicated_to': None,
                    'pending_replication': primary_node,
                    'transaction_id': transaction_id,
                    'results': {
                        central_node: {'success': True, 'method': 'atomic_insert_with_id_generation'}
                    },
                    'message': f'Atomic insert to {central_node} (fallback). Queued for {primary_node}.'
                }
                
            except Exception as e:
                conn.rollback()
                logger.error(f"✗ ATOMIC INSERT to CENTRAL failed: {e}")
                return {
                    'success': False,
                    'error': str(e),
                    'message': f'Atomic insert to {central_node} failed: {str(e)}'
                }
            finally:
                conn.close()

    def _get_new_tconst(self):
        """
        Generate new tconst from central node (quick separate transaction).
        Falls back to fragment nodes if central is unavailable.
        
        WARNING: Has a small race condition window between ID generation and insert.
        For atomic operations, use _get_new_tconst_transactional() instead.
        """
        # Try central first (has all data)
        central_node = 'node1'
        conn = self.db.get_connection(central_node, 'SERIALIZABLE')
        
        if conn:
            try:
                conn.start_transaction()
                cursor = conn.cursor(dictionary=True)
                
                cursor.execute("SELECT MAX(tconst) as max_id FROM titles FOR UPDATE")
                result = cursor.fetchone()
                max_tconst = result['max_id']
                
                if max_tconst is None:
                    new_tconst = 'tt0000001'
                else:
                    numeric_part = int(max_tconst[2:])
                    new_id = numeric_part + 1
                    new_tconst = f'tt{new_id:07d}'
                
                conn.commit()
                logger.info(f"Generated new tconst from central: {new_tconst}")
                return new_tconst
                
            except Exception as e:
                conn.rollback()
                logger.warning(f"Central unavailable for ID generation: {e}")
            finally:
                conn.close()
        
        # Fallback: get max from BOTH fragments and use the higher one
        logger.warning("Central unavailable, falling back to fragments for ID generation")
        
        max_tconst = None
        
        for node_name in ['node2', 'node3']:
            conn = self.db.get_connection(node_name, 'SERIALIZABLE')
            if conn:
                try:
                    conn.start_transaction()
                    cursor = conn.cursor(dictionary=True)
                    cursor.execute("SELECT MAX(tconst) as max_id FROM titles FOR UPDATE")
                    result = cursor.fetchone()
                    conn.commit()
                    
                    node_max = result['max_id']
                    if node_max:
                        if max_tconst is None or node_max > max_tconst:
                            max_tconst = node_max
                            
                    logger.info(f"Got max tconst from {node_name}: {node_max}")
                    
                except Exception as e:
                    logger.warning(f"Failed to get max from {node_name}: {e}")
                    try:
                        conn.rollback()
                    except:
                        pass
                finally:
                    conn.close()
        
        if max_tconst:
            numeric_part = int(max_tconst[2:])
            new_id = numeric_part + 1
            new_tconst = f'tt{new_id:07d}'
            logger.info(f"Generated new tconst from fragments: {new_tconst}")
            return new_tconst
        
        # No data anywhere - start fresh
        if max_tconst is None:
            logger.info("No existing tconst found, starting from tt0000001")
            return 'tt0000001'
        
        raise Exception("Cannot generate tconst: no nodes available")
    
    def update_title(self, tconst, data, isolation_level='READ COMMITTED'):
        """Update title with bidirectional replication support"""
        title = self.db.get_title_by_id(tconst)
        
        if 'error' in title:
            return {'success': False, 'error': 'Title not found'}
        
        title_type = title['title_type']
        primary_node = self._get_primary_node(title_type)
        central_node = 'node1'
        
        # Build UPDATE query (without last_updated - let MySQL set it)
        set_clauses = []
        params = []
        
        for key, value in data.items():
            if key != 'tconst':
                set_clauses.append(f"{key} = %s")
                params.append(value)
        
        params.append(tconst)
        query = f"UPDATE titles SET {', '.join(set_clauses)} WHERE tconst = %s"
        
        results = {}
        
        # Try primary fragment first
        result_primary = self.db.execute_query(primary_node, query, tuple(params), isolation_level)
        results[primary_node] = result_primary
        
        if result_primary['success']:
            logger.info(f"✓ UPDATE to PRIMARY {primary_node} succeeded for {tconst}")
            
            # Fetch updated record with new timestamp
            updated_record = self.db.get_title_by_id(tconst)
            if 'error' in updated_record:
                logger.error(f"✗ Failed to fetch updated record {tconst}")
                return {
                    'success': False,
                    'error': 'Update succeeded but failed to fetch record for replication'
                }
            
            last_updated = updated_record.get('last_updated')
            
            # Build replication query WITH explicit timestamp
            replication_set_clauses = []
            replication_params = []
            
            for key, value in data.items():
                if key != 'tconst':
                    replication_set_clauses.append(f"{key} = %s")
                    replication_params.append(value)
            
            # Add last_updated to SET clause
            replication_set_clauses.append("last_updated = %s")
            replication_params.append(last_updated)
            replication_params.append(tconst)
            
            replication_query = f"UPDATE titles SET {', '.join(replication_set_clauses)} WHERE tconst = %s"
            
            result_central = self.db.execute_query(central_node, replication_query, tuple(replication_params), isolation_level)
            results[central_node] = result_central
            
            if result_central['success']:
                self.transaction_logger.log_replication(
                    source_node=primary_node,
                    target_node=central_node,
                    operation_type='UPDATE',
                    record_id=tconst,
                    query=replication_query,
                    params=tuple(replication_params),
                    status='SUCCESS'
                )
                logger.info(f"✓ UPDATE replicated to CENTRAL {central_node} for {tconst}")
                
                return {
                    'success': True,
                    'primary_node': primary_node,
                    'replicated_to': central_node,
                    'results': results,
                    'message': f'Update committed and replicated'
                }
            else:
                transaction_id = self.transaction_logger.log_replication(
                    source_node=primary_node,
                    target_node=central_node,
                    operation_type='UPDATE',
                    record_id=tconst,
                    query=replication_query,
                    params=tuple(replication_params),
                    status='PENDING',
                    error_msg=result_central.get('error')
                )
                
                logger.warning(f"⚠ UPDATE REPLICATION FAILED for {tconst}. Queued.")
                
                return {
                    'success': True,
                    'primary_node': primary_node,
                    'replicated_to': None,
                    'pending_replication': central_node,
                    'transaction_id': transaction_id,
                    'results': results,
                    'message': f'Update committed to {primary_node}, replication queued'
                }
        else:
            # Fragment down - try central as fallback
            logger.warning(f"⚠ PRIMARY {primary_node} unavailable, using central as fallback for {tconst}")
            
            result_central = self.db.execute_query(central_node, query, tuple(params), isolation_level)
            results[central_node] = result_central
            
            if result_central['success']:
                # Fetch updated record from central
                updated_record = self.db.get_title_by_id(tconst)
                last_updated = updated_record.get('last_updated')
                
                # Build replication query with timestamp
                replication_set_clauses = []
                replication_params = []
                
                for key, value in data.items():
                    if key != 'tconst':
                        replication_set_clauses.append(f"{key} = %s")
                        replication_params.append(value)
                
                replication_set_clauses.append("last_updated = %s")
                replication_params.append(last_updated)
                replication_params.append(tconst)
                
                replication_query = f"UPDATE titles SET {', '.join(replication_set_clauses)} WHERE tconst = %s"
                
                transaction_id = self.transaction_logger.log_replication(
                    source_node=central_node,
                    target_node=primary_node,
                    operation_type='UPDATE',
                    record_id=tconst,
                    query=replication_query,
                    params=tuple(replication_params),
                    status='PENDING',
                    error_msg=f'{primary_node} was unavailable'
                )
                
                logger.info(f"✓ UPDATE to CENTRAL (fallback) succeeded for {tconst}. Queued for {primary_node}.")
                
                return {
                    'success': True,
                    'primary_node': central_node,
                    'replicated_to': None,
                    'pending_replication': primary_node,
                    'transaction_id': transaction_id,
                    'results': results,
                    'message': f'Update committed to {central_node} (fallback). Queued for {primary_node}.'
                }
            else:
                logger.error(f"✗ BOTH NODES FAILED for UPDATE {tconst}")
                return {
                    'success': False,
                    'error': 'All target nodes unavailable',
                    'results': results
                }
    
    def delete_title(self, tconst):
        """Delete title with bidirectional replication support"""
        title = self.db.get_title_by_id(tconst)
        
        if 'error' in title:
            return {'success': False, 'error': 'Title not found'}
        
        title_type = title['title_type']
        primary_node = self._get_primary_node(title_type)
        central_node = 'node1'
        
        query = "DELETE FROM titles WHERE tconst = %s"
        params = (tconst,)
        
        results = {}
        
        # Try primary fragment first
        result_primary = self.db.execute_query(primary_node, query, params)
        results[primary_node] = result_primary
        
        if result_primary['success']:
            logger.info(f"✓ DELETE from PRIMARY {primary_node} succeeded for {tconst}")
            
            result_central = self.db.execute_query(central_node, query, params)
            results[central_node] = result_central
            
            if result_central['success']:
                self.transaction_logger.log_replication(
                    source_node=primary_node,
                    target_node=central_node,
                    operation_type='DELETE',
                    record_id=tconst,
                    query=query,
                    params=params,
                    status='SUCCESS'
                )
                logger.info(f"✓ DELETE replicated to CENTRAL {central_node} for {tconst}")
                
                return {
                    'success': True,
                    'primary_node': primary_node,
                    'replicated_to': central_node,
                    'results': results,
                    'message': f'Delete committed and replicated'
                }
            else:
                transaction_id = self.transaction_logger.log_replication(
                    source_node=primary_node,
                    target_node=central_node,
                    operation_type='DELETE',
                    record_id=tconst,
                    query=query,
                    params=params,
                    status='PENDING',
                    error_msg=result_central.get('error')
                )
                
                logger.warning(f"⚠ DELETE REPLICATION FAILED for {tconst}. Queued.")
                
                return {
                    'success': True,
                    'primary_node': primary_node,
                    'replicated_to': None,
                    'pending_replication': central_node,
                    'transaction_id': transaction_id,
                    'results': results,
                    'message': f'Delete committed to {primary_node}, replication queued'
                }
        else:
            # Fragment down - try central as fallback
            logger.warning(f"⚠ PRIMARY {primary_node} unavailable, using central as fallback for {tconst}")
            
            result_central = self.db.execute_query(central_node, query, params)
            results[central_node] = result_central
            
            if result_central['success']:
                transaction_id = self.transaction_logger.log_replication(
                    source_node=central_node,
                    target_node=primary_node,
                    operation_type='DELETE',
                    record_id=tconst,
                    query=query,
                    params=params,
                    status='PENDING',
                    error_msg=f'{primary_node} was unavailable'
                )
                
                logger.info(f"✓ DELETE from CENTRAL (fallback) succeeded for {tconst}. Queued for {primary_node}.")
                
                return {
                    'success': True,
                    'primary_node': central_node,
                    'replicated_to': None,
                    'pending_replication': primary_node,
                    'transaction_id': transaction_id,
                    'results': results,
                    'message': f'Delete committed to {central_node} (fallback). Queued for {primary_node}.'
                }
            else:
                logger.error(f"✗ BOTH NODES FAILED for DELETE {tconst}")
                return {
                    'success': False,
                    'error': 'All target nodes unavailable',
                    'results': results
                }
    
    # === Delegation methods ===
    def recover_node(self, node_name):
        return self.recovery_handler.recover_node(node_name)
    
    def get_pending_replications(self):
        return self.recovery_handler.get_pending_summary()
    
    def test_concurrent_reads(self, tconst, isolation_level='READ COMMITTED'):
        return self.concurrency_tester.test_concurrent_reads(tconst, isolation_level)
    
    def test_concurrent_writes(self, updates, isolation_level='READ COMMITTED'):
        return self.concurrency_tester.test_concurrent_writes(updates, isolation_level)
    
    def test_read_write_conflict(self, tconst, new_data, isolation_level='READ COMMITTED'):
        return self.concurrency_tester.test_read_write_conflict(tconst, new_data, isolation_level)
    
    def simulate_failure(self, scenario):
        return self.concurrency_tester.simulate_failure(scenario)