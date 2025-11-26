import logging
import json
from datetime import datetime

logger = logging.getLogger(__name__)

class TransactionLogger:
    def __init__(self, db_manager):
        self.db = db_manager
    
    def log_replication(self, source_node, target_node, operation_type, 
                       record_id, query, params, status='PENDING', error_msg=None):
        """
        Log a replication operation to the source node's transaction_log.
        
        Args:
            source_node: node where write happened (node1/node2/node3)
            target_node: node where replica should go
            operation_type: INSERT/UPDATE/DELETE
            record_id: tconst value
            query: SQL query text
            params: Query parameters
            status: SUCCESS/FAILED/PENDING
            error_msg: Error message if failed
        """
        transaction_id = self._generate_transaction_id()
        
        log_query = """
            INSERT INTO transaction_log 
            (transaction_id, source_node, target_node, operation_type, 
             table_name, record_id, status, query_text, query_params, error_message)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Convert params tuple to JSON
        params_json = json.dumps(list(params)) if params else None
        
        log_params = (
            transaction_id,
            source_node,
            target_node,
            operation_type,
            'titles',
            record_id,
            status,
            query,
            params_json,
            error_msg
        )
        
        # Log to source node's transaction log
        result = self.db.execute_query(source_node, log_query, log_params)
        
        if not result['success']:
            logger.error(
                f"CRITICAL: Failed to log transaction on {source_node}! "
                f"Operation {operation_type} on {record_id} may be lost. "
                f"Error: {result.get('error')}"
            )
            return None
        
        logger.info(
            f"Logged {operation_type} for {record_id}: "
            f"{source_node} → {target_node} (status: {status})"
        )
        return transaction_id
    
    def update_log_status(self, node, transaction_id, status, error_msg=None):
        """Update status of a logged transaction"""
        query = """
            UPDATE transaction_log 
            SET status = %s, 
                error_message = %s,
                completed_at = NOW(),
                last_retry_at = NOW()
            WHERE transaction_id = %s
        """
        
        result = self.db.execute_query(node, query, (status, error_msg, transaction_id))
        
        if result['success']:
            logger.info(f"✓ Updated transaction {transaction_id} to {status}")
        else:
            logger.error(f"✗ Failed to update transaction {transaction_id}")
        
        return result
    
    def increment_retry_count(self, node, transaction_id):
        """Increment retry counter"""
        query = """
            UPDATE transaction_log 
            SET retry_count = retry_count + 1,
                last_retry_at = NOW()
            WHERE transaction_id = %s
        """
        return self.db.execute_query(node, query, (transaction_id,))
    
    def get_pending_replications(self, source_node):
        """Get all pending replications from a source node"""
        query = """
            SELECT * FROM transaction_log
            WHERE source_node = %s
              AND status = 'PENDING'
              AND retry_count < max_retries
            ORDER BY created_at ASC
        """
        
        conn = self.db.get_connection(source_node)
        if not conn:
            return []
        
        try:
            cursor = conn.cursor(dictionary=True)
            cursor.execute(query, (source_node,))
            results = cursor.fetchall()
            return results
        except Exception as e:
            logger.error(f"Error fetching pending replications from {source_node}: {e}")
            return []
        finally:
            conn.close()
    
    def _generate_transaction_id(self):
        """Generate unique transaction ID"""
        import uuid
        return str(uuid.uuid4())