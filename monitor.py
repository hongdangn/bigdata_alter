#!/usr/bin/env python3
"""
Real Estate Pipeline Monitoring Tool
Displays real-time statistics for Kafka, Elasticsearch, and the data pipeline
"""

import requests
import subprocess
import json
import time
import sys
from datetime import datetime


class PipelineMonitor:
    def __init__(self):
        self.kafka_bootstrap = "localhost:9092"
        self.es_host = "http://localhost:9200"
        self.kibana_host = "http://localhost:5601"
    
    def check_elasticsearch(self):
        """Check Elasticsearch status and document count"""
        try:
            # Cluster health
            health = requests.get(f"{self.es_host}/_cluster/health").json()
            
            # Document count
            count_response = requests.get(f"{self.es_host}/batdongsan/_count")
            count = count_response.json().get('count', 0) if count_response.status_code == 200 else 0
            
            # Index stats
            stats_response = requests.get(f"{self.es_host}/batdongsan/_stats")
            stats = {}
            if stats_response.status_code == 200:
                stats_data = stats_response.json()
                indices = stats_data.get('indices', {}).get('batdongsan', {})
                total = indices.get('total', {})
                stats = {
                    'size': total.get('store', {}).get('size_in_bytes', 0) / 1024 / 1024,  # MB
                    'docs_deleted': total.get('docs', {}).get('deleted', 0)
                }
            
            return {
                'status': health.get('status', 'unknown'),
                'nodes': health.get('number_of_nodes', 0),
                'document_count': count,
                'index_size_mb': stats.get('size', 0),
                'docs_deleted': stats.get('docs_deleted', 0)
            }
        except Exception as e:
            return {'error': str(e)}
    
    def check_kafka(self):
        """Check Kafka topics and consumer lag"""
        try:
            # List topics
            result = subprocess.run(
                ['docker', 'exec', 'kafka', 'kafka-topics',
                 '--list', '--bootstrap-server', 'localhost:9093'],
                capture_output=True, text=True, timeout=5
            )
            topics = result.stdout.strip().split('\n')
            
            # Get consumer groups
            groups_result = subprocess.run(
                ['docker', 'exec', 'kafka', 'kafka-consumer-groups',
                 '--list', '--bootstrap-server', 'localhost:9093'],
                capture_output=True, text=True, timeout=5
            )
            groups = groups_result.stdout.strip().split('\n')
            
            return {
                'topics': [t for t in topics if t],
                'consumer_groups': [g for g in groups if g],
                'topic_count': len([t for t in topics if t])
            }
        except Exception as e:
            return {'error': str(e)}
    
    def check_docker_containers(self):
        """Check Docker container status"""
        try:
            result = subprocess.run(
                ['docker-compose', 'ps', '--format', 'json'],
                capture_output=True, text=True, timeout=5
            )
            
            containers = []
            for line in result.stdout.strip().split('\n'):
                if line:
                    try:
                        container = json.loads(line)
                        containers.append({
                            'name': container.get('Service', 'unknown'),
                            'state': container.get('State', 'unknown'),
                            'status': container.get('Status', 'unknown')
                        })
                    except:
                        pass
            
            return containers
        except Exception as e:
            return {'error': str(e)}
    
    def get_recent_documents(self, limit=5):
        """Get most recent documents from Elasticsearch"""
        try:
            query = {
                "size": limit,
                "sort": [{"processed_at": {"order": "desc"}}],
                "_source": ["title", "price", "address", "processed_at"]
            }
            
            response = requests.post(
                f"{self.es_host}/batdongsan/_search",
                json=query
            )
            
            if response.status_code == 200:
                hits = response.json().get('hits', {}).get('hits', [])
                return [hit['_source'] for hit in hits]
            return []
        except Exception as e:
            return []
    
    def display_status(self):
        """Display comprehensive pipeline status"""
        print("\n" + "="*70)
        print(f"Real Estate Pipeline Status - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*70)
        
        # Docker Containers
        print("\n📦 Docker Containers:")
        print("-" * 70)
        containers = self.check_docker_containers()
        if isinstance(containers, list):
            for container in containers:
                status_symbol = "✓" if container['state'] == 'running' else "✗"
                print(f"  {status_symbol} {container['name']:<20} {container['state']:<15} {container.get('status', '')}")
        else:
            print(f"  Error: {containers.get('error', 'Unknown error')}")
        
        # Kafka Status
        print("\n📨 Kafka Status:")
        print("-" * 70)
        kafka_info = self.check_kafka()
        if 'error' not in kafka_info:
            print(f"  Topics: {kafka_info['topic_count']}")
            if kafka_info['topics']:
                for topic in kafka_info['topics'][:5]:
                    print(f"    - {topic}")
            print(f"  Consumer Groups: {len(kafka_info['consumer_groups'])}")
        else:
            print(f"  Error: {kafka_info['error']}")
        
        # Elasticsearch Status
        print("\n🔍 Elasticsearch Status:")
        print("-" * 70)
        es_info = self.check_elasticsearch()
        if 'error' not in es_info:
            status_symbol = "✓" if es_info['status'] == 'green' else ("⚠" if es_info['status'] == 'yellow' else "✗")
            print(f"  Cluster Status: {status_symbol} {es_info['status'].upper()}")
            print(f"  Nodes: {es_info['nodes']}")
            print(f"  Documents: {es_info['document_count']:,}")
            print(f"  Index Size: {es_info['index_size_mb']:.2f} MB")
            if es_info['docs_deleted'] > 0:
                print(f"  Deleted Docs: {es_info['docs_deleted']}")
        else:
            print(f"  Error: {es_info['error']}")
        
        # Recent Documents
        print("\n📄 Recent Documents:")
        print("-" * 70)
        recent_docs = self.get_recent_documents(3)
        if recent_docs:
            for i, doc in enumerate(recent_docs, 1):
                print(f"  {i}. {doc.get('title', 'N/A')[:50]}")
                print(f"     Price: {doc.get('price', 'N/A')}, Address: {doc.get('address', 'N/A')[:30]}")
        else:
            print("  No documents found")
        
        # Service URLs
        print("\n🌐 Service URLs:")
        print("-" * 70)
        print(f"  Kibana:        {self.kibana_host}")
        print(f"  Elasticsearch: {self.es_host}")
        print(f"  Kafka:         {self.kafka_bootstrap}")
        
        print("\n" + "="*70)
    
    def monitor_loop(self, interval=10):
        """Continuously monitor the pipeline"""
        try:
            while True:
                # Clear screen (works on most terminals)
                print("\033[2J\033[H", end="")
                self.display_status()
                print(f"\nRefreshing in {interval} seconds... (Press Ctrl+C to exit)")
                time.sleep(interval)
        except KeyboardInterrupt:
            print("\n\nMonitoring stopped.")
            sys.exit(0)


def main():
    monitor = PipelineMonitor()
    
    if len(sys.argv) > 1 and sys.argv[1] == '--loop':
        interval = int(sys.argv[2]) if len(sys.argv) > 2 else 10
        monitor.monitor_loop(interval)
    else:
        monitor.display_status()
        print("\nTip: Run with '--loop [seconds]' for continuous monitoring")
        print("Example: python monitor.py --loop 5")


if __name__ == "__main__":
    main()