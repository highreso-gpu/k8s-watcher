import requests
import json
import logging
from typing import Dict, Any, Optional
import ssl
import requests
import os

CERTS_CERTFILE=os.getenv('CERTS_CERTFILE', "/docker/certs/default.crt")
CERTS_KEYFILE=os.getenv('CERTS_KEYFILE', "/docker/certs/default.key")

class ClusterApiClient:
    def __init__(self, base_url: str, api_key: Optional[str] = None, timeout: int = 30):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.session = requests.Session()
        self.logger = logging.getLogger(__name__)

        # 認証ヘッダーの設定
        if self.api_key:
            self.session.headers.update({
                'Authorization': f'Bearer {self.api_key}',
                'Content-Type': 'application/json'
            })

    def pod_event(self, pod_data: Dict[str, Any]) -> bool:
        """
        clusterapiのDB更新エンドポイントを呼び出し

        Args:
            pod_data: Pod情報のデータ

        Returns:
            bool: 更新成功の場合True
        """
        # SSLコンテキストを構築
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_context.load_cert_chain(CERTS_CERTFILE, CERTS_KEYFILE)
        url = f"{self.base_url}/instance/updateStatusByK8sWatcher"
        try:
            self.logger.info(f"Calling clusterapi: {url}")
            # SSLコンテキストを直接利用する方法は requestsではサポートされていませんが、`verify=False`で検証をスキップできます。        
            response = requests.post(url, json=pod_data, verify=False, timeout=10)  
            pod_name = pod_data.get('instance', {}).get('pod_name', 'unknown')
            if response.status_code == 200:
                self.logger.info(f"Successfully updated pod data for {pod_name}")
                return True
            else:
                self.logger.error(f"Failed to update pod data. Status: {response.status_code}, Response: {response.text}")
                return False

        except requests.exceptions.ConnectionError:
            self.logger.error(f"Connection error: Unable to connect to clusterapi at {url}")
            return False
        except requests.exceptions.Timeout:
            self.logger.error(f"Timeout error: Request to {url} timed out")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error calling clusterapi: {e}")
            return False

    def pvc_event(self, pvc_data: Dict[str, Any]) -> bool:
        """
        PVCイベントをclusterapiに通知

        Args:
            pvc_data: PVC情報のデータ

        Returns:
            bool: 通知成功の場合True
        """
        # SSLコンテキストを構築
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_context.load_cert_chain(CERTS_CERTFILE, CERTS_KEYFILE)
        # PVCイベント通知エンドポイント
        if not pvc_data:
            self.logger.error("No PVC data provided for notification")
            return False
        url = f"{self.base_url}/instance/notifyPVCByK8sWatcher"
        try:
            self.logger.info(f"Calling clusterapi for PVC event: {url}")
            response = requests.post(url, json=pvc_data, verify=False, timeout=10)
            pvc_name = pvc_data.get('metadata', {}).get('name', 'unknown')

            if response.status_code == 200:
                self.logger.info(f"Successfully notified clusterapi about PVC event for {pvc_name}")
                return True
            else:
                self.logger.error(f"Failed to notify clusterapi about PVC event. Status: {response.status_code}, Response: {response.text}")
                return False
        except Exception as e:
            self.logger.error(f"Error notifying clusterapi about PVC event: {e}")
            return False
    def health_check(self) -> bool:
        """clusterapiの疎通確認"""
        try:
            response = self.session.get(f"{self.base_url}/health", timeout=5)
            return response.status_code == 200
        except:
            return False
