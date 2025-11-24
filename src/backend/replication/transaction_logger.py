import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# TODO: give each node its own transaction log table for better tracking

class TransactionLogger:
    def __init__(self, db_manager):
        self.db = db_manager
    
    def log_transaction(self, node_id, operation_type, record_id, 
                       status='SUCCESS', error_msg=None, table_name='titles'):
        """Log transaction to database"""
        query = """
            INSERT INTO transaction_log 
            (transaction_id, node_id, operation_type, table_name, record_id, status, error_message)
            VALUES (UUID(), %s, %s, %s, %s, %s, %s)
        """
        result = self.db.execute_query(
            'node1',
            query,
            (node_id, operation_type, table_name, record_id, status, error_msg)
        )
        if not result['success']:
            logger.error(f"Failed to log transaction: {result.get('error')}")