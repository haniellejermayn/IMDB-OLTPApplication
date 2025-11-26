import logging
import json
from .transaction_logger import TransactionLogger
from .recovery_handler import RecoveryHandler
from .concurrency_tester import ConcurrencyTester

logger = logging.getLogger(__name__)

class ReplicationManager:
    def __init__(self, db_manager):
        self.db = db_manager
        self.transaction_logger = TransactionLogger(db_manager)
        self.recovery_handler = RecoveryHandler(db_manager, self.transaction_logger)
        self.concurrency_tester = ConcurrencyTester(db_manager, self)
    
    def _get_primary_node(self, title_type):
        return 'node2' if title_type == 'movie' else 'node3'
    
    def insert_title(self, data):
        """
        Insert flow with bidirectional support:
        1. Try primary fragment (Node 2 or 3)
        2. If fragment down, use central as fallback
        3. Replicate to the other node
        4. Log everything for recovery
        """
        tconst = data.get('tconst')
        title_type = data.get('title_type')
        
        primary_node = self._get_primary_node(title_type)
        central_node = 'node1'
        
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
        
        # Try primary fragment first
        result_primary = self.db.execute_query(primary_node, query, params)
        results[primary_node] = result_primary
        
        if result_primary['success']:
            # Fragment succeeded - replicate to central
            logger.info(f"✓ INSERT to PRIMARY {primary_node} succeeded for {tconst}")
            
            result_central = self.db.execute_query(central_node, query, params)
            results[central_node] = result_central
            
            if result_central['success']:
                self.transaction_logger.log_replication(
                    source_node=primary_node,
                    target_node=central_node,
                    operation_type='INSERT',
                    record_id=tconst,
                    query=query,
                    params=params,
                    status='SUCCESS'
                )
                logger.info(f"✓ INSERT replicated to CENTRAL {central_node} for {tconst}")
                
                return {
                    'success': True,
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
                    query=query,
                    params=params,
                    status='PENDING',
                    error_msg=result_central.get('error')
                )
                
                logger.warning(f"⚠ REPLICATION FAILED: {primary_node} → {central_node} for {tconst}. Queued.")
                
                return {
                    'success': True,
                    'primary_node': primary_node,
                    'replicated_to': None,
                    'pending_replication': central_node,
                    'transaction_id': transaction_id,
                    'results': results,
                    'message': f'Insert committed to {primary_node}. Replication to {central_node} queued.'
                }
        else:
            # Fragment down - try central as fallback
            logger.warning(f"⚠ PRIMARY {primary_node} unavailable, using central as fallback for {tconst}")
            
            result_central = self.db.execute_query(central_node, query, params)
            results[central_node] = result_central
            
            if result_central['success']:
                # Central worked - queue replication to fragment
                transaction_id = self.transaction_logger.log_replication(
                    source_node=central_node,
                    target_node=primary_node,
                    operation_type='INSERT',
                    record_id=tconst,
                    query=query,
                    params=params,
                    status='PENDING',
                    error_msg=f'{primary_node} was unavailable'
                )
                
                logger.info(f"✓ INSERT to CENTRAL (fallback) succeeded for {tconst}. Queued for {primary_node}.")
                
                return {
                    'success': True,
                    'primary_node': central_node,
                    'replicated_to': None,
                    'pending_replication': primary_node,
                    'transaction_id': transaction_id,
                    'results': results,
                    'message': f'Insert committed to {central_node} (fallback). Queued for {primary_node}.'
                }
            else:
                # Both nodes failed
                logger.error(f"✗ BOTH NODES FAILED for INSERT {tconst}")
                return {
                    'success': False,
                    'error': 'All target nodes unavailable',
                    'results': results,
                    'message': f'Insert failed: both {primary_node} and {central_node} unavailable'
                }
    
    def update_title(self, tconst, data, isolation_level='READ COMMITTED'):
        """Update title with bidirectional replication support"""
        title = self.db.get_title_by_id(tconst)
        
        if 'error' in title:
            return {'success': False, 'error': 'Title not found'}
        
        title_type = title['title_type']
        primary_node = self._get_primary_node(title_type)
        central_node = 'node1'
        
        # Build query
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
            # Fragment succeeded - replicate to central
            logger.info(f"✓ UPDATE to PRIMARY {primary_node} succeeded for {tconst}")
            
            result_central = self.db.execute_query(central_node, query, tuple(params), isolation_level)
            results[central_node] = result_central
            
            if result_central['success']:
                self.transaction_logger.log_replication(
                    source_node=primary_node,
                    target_node=central_node,
                    operation_type='UPDATE',
                    record_id=tconst,
                    query=query,
                    params=tuple(params),
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
                    query=query,
                    params=tuple(params),
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
                transaction_id = self.transaction_logger.log_replication(
                    source_node=central_node,
                    target_node=primary_node,
                    operation_type='UPDATE',
                    record_id=tconst,
                    query=query,
                    params=tuple(params),
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