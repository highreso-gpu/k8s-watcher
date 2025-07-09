from kubernetes import client, config, watch
from kubernetes.config import ConfigException
import yaml
import logging
import os
from datetime import datetime
from typing import Dict, Any, List, Optional
from .clusterapi_client import ClusterApiClient

class PodWatcher:
    def __init__(self, environment: str = "development"):
        self.environment = environment
        self.config = self._load_environment_config()
        # self.clusterapi_client = self._setup_clusterapi_client()
        self.v1 = None
        self.watch = watch.Watch()
        self._setup_logging()

    def _load_environment_config(self) -> Dict[str, Any]:
        """環境別設定ファイルを読み込み"""
        # ベース設定を読み込み
        base_config = self._load_config_file("config/base.yaml")

        # 環境別設定を読み込み
        env_config_file = f"config/{self.environment}.yaml"
        env_config = self._load_config_file(env_config_file)

        # 設定をマージ（環境別設定が優先）
        merged_config = self._merge_configs(base_config, env_config)

        # 環境変数を置換
        return self._substitute_env_vars(merged_config)

    def _load_config_file(self, config_file: str) -> Dict[str, Any]:
        """設定ファイルを読み込み"""
        try:
            with open(config_file, 'r') as file:
                return yaml.safe_load(file) or {}
        except FileNotFoundError:
            self.logger.warning(f"Config file {config_file} not found") if hasattr(self, 'logger') else print(f"Config file {config_file} not found")
            return {}
        except Exception as e:
            error_msg = f"Error loading config {config_file}: {e}"
            self.logger.error(error_msg) if hasattr(self, 'logger') else print(error_msg)
            return {}

    def _merge_configs(self, base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """設定をマージ（再帰的）"""
        result = base.copy()

        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._merge_configs(result[key], value)
            else:
                result[key] = value

        return result

    def _substitute_env_vars(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """環境変数を置換"""
        def substitute_recursive(obj):
            if isinstance(obj, dict):
                return {k: substitute_recursive(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [substitute_recursive(item) for item in obj]
            elif isinstance(obj, str) and obj.startswith('${') and obj.endswith('}'):
                env_var = obj[2:-1]
                default_value = ""
                if ":-" in env_var:
                    env_var, default_value = env_var.split(":-", 1)
                return os.getenv(env_var, default_value)
            else:
                return obj

        return substitute_recursive(config)

    def _setup_logging(self):
        """ログ設定"""
        log_level = getattr(logging, self.config.get('watcher', {}).get('log_level', 'INFO'))

        # 環境別のログフォーマット
        if self.environment == 'production':
            # 本番環境：構造化ログ
            log_format = '{"timestamp":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","message":"%(message)s","environment":"' + self.environment + '"}'
        else:
            # 開発環境：読みやすいフォーマット
            log_format = f'[{self.environment.upper()}] %(asctime)s - %(name)s - %(levelname)s - %(message)s'

        logging.basicConfig(
            level=log_level,
            format=log_format
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"Starting k8s-watcher in {self.environment} environment")

    def _setup_clusterapi_client(self) -> ClusterApiClient:
        """ClusterAPI クライアントのセットアップ"""
        clusterapi_config = self.config.get('clusterapi', {})
        base_url = clusterapi_config.get('base_url', 'http://localhost:3000')
        api_key = clusterapi_config.get('auth', {}).get('api_key')
        timeout = clusterapi_config.get('timeout', 30)
        self.logger.info(f"Setting up ClusterAPI client: {base_url}")
        if api_key:
            self.logger.debug("API key provided for authentication")
        else:
            self.logger.debug("No API key provided")

        return ClusterApiClient(base_url, api_key, timeout)

    def setup_k8s_client(self) -> bool:
        """Kubernetesクライアントのセットアップ（環境別）"""
        try:
            k8s_config = self.config.get('kubernetes', {})

            if k8s_config.get('use_incluster_config', False):
                # クラスタ内で実行（ステージング・本番）
                self.logger.info("Using in-cluster configuration")
                config.load_incluster_config()

            elif 'config_file' in k8s_config:
                # kubeconfigファイルを使用（ローカル・外部アクセス）
                kubeconfig_path = k8s_config['config_file']
                self.logger.info(f"Loading kubeconfig from: {kubeconfig_path}")

                if not os.path.exists(kubeconfig_path):
                    self.logger.error(f"Kubeconfig file not found: {kubeconfig_path}")
                    return False

                config.load_kube_config(config_file=kubeconfig_path)

            else:
                # デフォルトのkubeconfig
                self.logger.info("Using default kubeconfig")
                config.load_kube_config()

            # APIクライアントを作成
            self.v1 = client.CoreV1Api()

            # 接続テスト
            api_version = self.v1.get_api_version()
            self.logger.info(f"Successfully connected to Kubernetes API version: {api_version}")

            # 環境別の追加チェック
            if self.environment != 'local':
                # 実際のK8s環境での追加チェック
                namespaces = self.v1.list_namespace()
                namespace_names = [ns.metadata.name for ns in namespaces.items[:5]]  # 最初の5個だけログ出力
                self.logger.info(f"Sample namespaces: {namespace_names}")

            return True

        except ConfigException as e:
            self.logger.error(f"Kubernetes config error: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Error setting up k8s client: {e}")
            return False

    def _extract_pod_data(self, pod) -> Dict[str, Any]:
        """Podオブジェクトからclusterapiに送信するデータを抽出"""
        base_data = {
            "name": pod.metadata.name,
            "namespace": pod.metadata.namespace,
            "uid": pod.metadata.uid,
            "environment": self.environment,  # 環境情報を追加
            "status": {
                "phase": pod.status.phase if pod.status else "Unknown",
                "conditions": [
                    {
                        "type": condition.type,
                        "status": condition.status,
                        "reason": condition.reason,
                        "message": condition.message
                    } for condition in (pod.status.conditions or [])
                ] if pod.status else [],
                "container_statuses": [
                    {
                        "name": cs.name,
                        "ready": cs.ready,
                        "restart_count": cs.restart_count,
                        "state": str(cs.state) if cs.state else None
                    } for cs in (pod.status.container_statuses or [])
                ] if pod.status else []
            },
            "spec": {
                "node_name": pod.spec.node_name if pod.spec else None,
                "containers": [
                    {
                        "name": container.name,
                        "image": container.image
                    } for container in (pod.spec.containers or [])
                ] if pod.spec else []
            },
            "metadata": {
                "labels": pod.metadata.labels or {},
                "annotations": pod.metadata.annotations or {},
                "creation_timestamp": pod.metadata.creation_timestamp.isoformat() if pod.metadata.creation_timestamp else None
            },
            "event_timestamp": datetime.now().isoformat()
        }

        return base_data

    def should_process_event(self, event_type: str, pod) -> bool:
        """イベントを処理すべきかどうかを判定"""
        # 本番環境では重要なイベントのみ処理
        if self.environment == 'production':
            critical_only = self.config.get('watcher', {}).get('alerts', {}).get('critical_events_only', False)
            if critical_only and event_type not in ['DELETED'] and pod.status and pod.status.phase not in ['Failed', 'Succeeded']:
                return False

        return True

    def handle_pod_event(self, event_type: str, pod):
        """Podイベントの処理"""
        pod_name = pod.metadata.name
        namespace = pod.metadata.namespace

        # イベント処理の判定
        if not self.should_process_event(event_type, pod):
            return

        self.logger.info(f"Pod event detected: {event_type} - {namespace}/{pod_name}")

        # 監視対象のnamespaceかチェック
        target_namespaces = self.config.get('watcher', {}).get('namespaces', [])
        if target_namespaces and namespace not in target_namespaces:
            self.logger.debug(f"Skipping pod {namespace}/{pod_name} - not in target namespaces")
            return

        # Podデータを抽出
        pod_data = self._extract_pod_data(pod)
        pod_data['event_type'] = event_type

        # clusterapiのDB更新エンドポイントを呼び出し
        # success = self.clusterapi_client.update_pod_status(pod_data)

        # if success:
        #     self.logger.info(f"Successfully notified clusterapi about {event_type} event for {namespace}/{pod_name}")
        # else:
        #     self.logger.error(f"Failed to notify clusterapi about {event_type} event for {namespace}/{pod_name}")

    def start_watching(self):
        """Pod監視を開始"""
        if not self.setup_k8s_client():
            self.logger.error("Failed to setup Kubernetes client")
            return

        # clusterapiの疎通確認
        # if not self.clusterapi_client.health_check():
        #     self.logger.warning("ClusterAPI health check failed, but continuing...")
        # else:
        #     self.logger.info("ClusterAPI health check passed")

        self.logger.info(f"Starting Pod watcher in {self.environment} environment...")
        target_namespaces = self.config.get('watcher', {}).get('namespaces', [])
        if target_namespaces:
            self.logger.info(f"Monitoring namespaces: {target_namespaces}")
        else:
            self.logger.info("Monitoring all namespaces")

        try:
            # Podの監視を開始
            stream = self.watch.stream(self.v1.list_pod_for_all_namespaces)

            for event in stream:
                event_type = event['type']
                pod = event['object']
                self.handle_pod_event(event_type, pod)

        except KeyboardInterrupt:
            self.logger.info("Stopping Pod watcher...")
        except Exception as e:
            self.logger.error(f"Error in Pod watcher: {e}")
            raise
        finally:
            self.watch.stop()
