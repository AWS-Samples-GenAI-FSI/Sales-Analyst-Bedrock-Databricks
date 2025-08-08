"""
Databricks REST API connector (alternative to SQL connector)
"""
import os
import requests
import time
from dotenv import load_dotenv

load_dotenv()

class DatabricksRestConnector:
    def __init__(self):
        self.host = os.getenv('DATABRICKS_HOST', '').rstrip('/')
        self.token = os.getenv('DATABRICKS_TOKEN', '')
        self.warehouse_id = os.getenv('DATABRICKS_CLUSTER_ID', '')
        
        self.headers = {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json'
        }
    
    def execute_query(self, query):
        """Execute SQL query using REST API"""
        try:
            # Start query execution
            response = requests.post(
                f"{self.host}/api/2.0/sql/statements/",
                headers=self.headers,
                json={
                    "warehouse_id": self.warehouse_id,
                    "statement": query,
                    "wait_timeout": "30s"
                }
            )
            
            if response.status_code != 200:
                raise Exception(f"Query failed: {response.text}")
            
            result = response.json()
            statement_id = result.get('statement_id')
            
            # Poll for completion if still running
            while result.get('status', {}).get('state') in ['PENDING', 'RUNNING']:
                time.sleep(2)
                response = requests.get(
                    f"{self.host}/api/2.0/sql/statements/{statement_id}",
                    headers=self.headers
                )
                result = response.json()
            
            # Extract results
            if result.get('status', {}).get('state') == 'SUCCEEDED':
                data = result.get('result', {}).get('data_array', [])
                columns = [col['name'] for col in result.get('manifest', {}).get('schema', {}).get('columns', [])]
                
                # Convert to list of dictionaries
                return [dict(zip(columns, row)) for row in data]
            else:
                error_msg = result.get('status', {}).get('error', {}).get('message', 'Unknown error')
                raise Exception(f"Query failed: {error_msg}")
                
        except Exception as e:
            print(f"Error executing query: {e}")
            return []

# Test function
def test_rest_connection():
    connector = DatabricksRestConnector()
    result = connector.execute_query("SELECT 1 as test")
    print(f"Test result: {result}")
    return len(result) > 0

if __name__ == "__main__":
    test_rest_connection()