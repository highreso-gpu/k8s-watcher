import requests
import json
import logging
from typing import Dict, Any, Optional

class ClusterApiClient:
    def __init__(self, base_url: str, api_key: Optional[str] = None):
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

    def update_pod_status(self, pod_data: Dict[str, Any]) -> bool:
        """
        clusterapiのDB更新エンドポイントを呼び出し

        Args:
            pod_data: Pod情報のデータ

        Returns:
            bool: 更新成功の場合True
        """
        endpoint = f"{self.base_url}/api/pods/update"

        try:
            self.logger.info(f"Calling clusterapi: {endpoint}")
            self.logger.debug(f"Pod data: {json.dumps(pod_data, indent=2)}")

            response = self.session.post(endpoint, json=pod_data)

            if response.status_code == 200:
                self.logger.info(f"Successfully updated pod data for {pod_data.get('name', 'unknown')}")
                return True
            else:
                self.logger.error(f"Failed to update pod data. Status: {response.status_code}, Response: {response.text}")
                return False

        except requests.exceptions.ConnectionError:
            self.logger.error(f"Connection error: Unable to connect to clusterapi at {endpoint}")
            return False
        except requests.exceptions.Timeout:
            self.logger.error(f"Timeout error: Request to {endpoint} timed out")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error calling clusterapi: {e}")
            return False

    def health_check(self) -> bool:
        """clusterapiの疎通確認"""
        try:
            response = self.session.get(f"{self.base_url}/health", timeout=5)
            return response.status_code == 200
        except:
            return False
