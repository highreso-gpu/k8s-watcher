from kubernetes import client, config, watch
from kubernetes.client import Configuration
from kubernetes.config import ConfigException
import os
import logging
import time
from datetime import datetime
from typing import Dict, Any, Optional
import threading
import yaml
from .clusterapi_client import ClusterApiClient

class MultiClusterPodPhaseWatcher:
    def __init__(self, environment: str = "development"):
        self.environment = environment
        self.config = self._load_environment_config()
        self.clusterapi_client = self._setup_clusterapi_client()
        self._setup_logging()

        # Pod phase の履歴を保持（重複通知防止用）
        self.pod_phase_history = {}

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
            if hasattr(self, 'logger'):
                self.logger.warning(f"Config file {config_file} not found")
            else:
                print(f"Config file {config_file} not found")
            return {}
        except Exception as e:
            error_msg = f"Error loading config {config_file}: {e}"
            if hasattr(self, 'logger'):
                self.logger.error(error_msg)
            else:
                print(error_msg)
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
    
    def _setup_clusterapi_client(self) -> ClusterApiClient:
        """ClusterAPI クライアントのセットアップ"""
        clusterapi_config = self.config.get('clusterapi', {})
        base_url = clusterapi_config.get('base_url', 'http://localhost:3000')
        api_key = clusterapi_config.get('auth', {}).get('api_key')
        timeout = clusterapi_config.get('timeout', 30)
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"Setting up ClusterApiClient with base URL: {base_url}")

        return ClusterApiClient(base_url, api_key, timeout)

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
        self.logger.info(f"Starting pod phase k8s-watcher in {self.environment} environment")

    def _extract_pod_phase_data(self, pod) -> Dict[str, Any]:
        """Pod phaseに特化したデータ抽出"""
        current_phase = pod.status.phase if pod.status else "Unknown"

        # インスタンス情報の抽出
        instance_info = {
            "pod_name": pod.metadata.name,
            "namespace": pod.metadata.namespace,
            "uid": pod.metadata.uid,
            "node_name": pod.spec.node_name if pod.spec else None,
            "creation_timestamp": pod.metadata.creation_timestamp.isoformat() if pod.metadata.creation_timestamp else None
        }

        # Phase情報の詳細
        phase_info = {
            "current_phase": current_phase,
            "previous_phase": self.pod_phase_history.get(pod.metadata.uid, {}).get("phase"),
            "phase_change_timestamp": datetime.now().isoformat(),
            "conditions": []
        }

        # Podの状態詳細を取得
        if pod.status and pod.status.conditions:
            for condition in pod.status.conditions:
                phase_info["conditions"].append({
                    "type": condition.type,
                    "status": condition.status,
                    "reason": condition.reason,
                    "message": condition.message,
                    "last_transition_time": condition.last_transition_time.isoformat() if condition.last_transition_time else None
                })

        # コンテナの状態情報
        container_info = []
        if pod.status and pod.status.container_statuses:
            for cs in pod.status.container_statuses:
                container_info.append({
                    "name": cs.name,
                    "ready": cs.ready,
                    "restart_count": cs.restart_count,
                    "state": self._get_container_state(cs.state) if cs.state else "Unknown"
                })

        return {
            "instance": instance_info,
            "phase": phase_info,
            "containers": container_info,
            "environment": self.environment,
            "event_timestamp": datetime.now().isoformat()
        }

    def _get_container_state(self, state) -> str:
        """コンテナの状態を文字列で取得"""
        if state.running:
            return "Running"
        elif state.waiting:
            return f"Waiting ({state.waiting.reason})"
        elif state.terminated:
            return f"Terminated ({state.terminated.reason})"
        else:
            return "Unknown"

    def _should_notify_phase_change(self, pod_uid: str, current_phase: str) -> bool:
        """Phase変更を通知すべきかどうかを判定"""
        if pod_uid not in self.pod_phase_history:
            # 初回は通知
            return True

        previous_phase = self.pod_phase_history[pod_uid].get("phase")
        if previous_phase != current_phase:
            # Phase変更があった場合は通知
            return True

        # 同じPhaseの場合は通知しない（重複防止）
        return False

    def _update_phase_history(self, pod_uid: str, phase: str):
        """Pod phaseの履歴を更新"""
        self.pod_phase_history[pod_uid] = {
            "phase": phase,
            "timestamp": datetime.now().isoformat()
        }

    def setup_k8s_client(self) -> bool:
        """Kubernetesクライアントのセットアップ（APIサーバーホスト指定対応）"""
        try:
            k8s_config = self.config.get('kubernetes', {})

            api_host = k8s_config['host']
            verify_ssl = k8s_config.get('verify_ssl', True)
            token = k8s_config.get('token')

            self.logger.info(f"Connecting to Kubernetes API server at: {api_host}")

            # カスタム設定を作成
            configuration = Configuration()
            configuration.host = api_host
            configuration.verify_ssl = verify_ssl

            # 認証トークンの設定
            configuration.api_key = {"authorization": f"Bearer {token}"}
            # デフォルト設定として適用
            client.Configuration.set_default(configuration)
            return True

        except ConfigException as e:
            self.logger.error(f"Kubernetes config error: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Error setting up k8s client: {e}")
            return False

    def start_watching_all_clusters(self):
        """すべてのクラスタの監視を開始"""
        clusters = self.config.get('kubernetes', {}).get('clusters', [])

        if not clusters:
            self.logger.error("No clusters configured")
            return

        threads = []

        for cluster_config in clusters:
            cluster_name = cluster_config.get('name')
            self.logger.info(f"Starting watcher for cluster: {cluster_name}")

            # 各クラスタ用のスレッドを作成
            thread = threading.Thread(
                target=self._watch_cluster,
                args=(cluster_config,),
                name=f"watcher-{cluster_name}"
            )
            thread.daemon = True
            thread.start()
            threads.append(thread)
            

        # すべてのスレッドの終了を待機
        for thread in threads:
            thread.join()
    
    def _watch_cluster(self, cluster_config: Dict):
        """単一クラスタの監視"""
        cluster_name = cluster_config.get('name')

        try:
            # クラスタ固有のクライアントを設定
            v1_client = self._setup_cluster_client(cluster_config)
            watch_instance = watch.Watch()

            self.logger.info(f"Starting Pod monitoring for cluster: {cluster_name}")

            # Podの監視
            pod_stream = watch_instance.stream(v1_client.list_pod_for_all_namespaces)

            # PVCの監視
            pvc_stream = watch_instance.stream(v1_client.list_persistent_volume_claim_for_all_namespaces)

            def watch_pods():
                for event in pod_stream:
                    event_type = event['type']
                    pod = event['object']
                    self.handle_pod_phase_event(event_type, pod, cluster_name)
            
            def watch_pvcs():
                for event in pvc_stream:
                    event_type = event['type']
                    pvc = event['object']
                    self.handle_pvc_event(event_type, pvc, cluster_name)
            pod_thread = threading.Thread(target=watch_pods)
            pvc_thread = threading.Thread(target=watch_pvcs)
            pod_thread.start()
            pvc_thread.start()
            pod_thread.join()
            pvc_thread.join()

        except Exception as e:
            self.logger.error(f"Error watching cluster {cluster_name}: {e}")
        finally:
            if 'watch_instance' in locals():
                watch_instance.stop()

    def _setup_cluster_client(self, cluster_config: Dict):
        """クラスタ固有のKubernetesクライアントを設定"""
        cluster_name = cluster_config.get('name')

        try:
            # 直接接続設定
            configuration = Configuration()
            configuration.host = cluster_config['host']
            configuration.verify_ssl = cluster_config.get('verify_ssl', True)

            if cluster_config.get('token'):
                configuration.api_key = {
                    "authorization": f"Bearer {cluster_config['token']}"
                }

                client.Configuration.set_default(configuration)

            return client.CoreV1Api()

        except Exception as e:
            self.logger.error(f"Failed to setup client for cluster {cluster_name}: {e}")
            raise

    def handle_pod_phase_event(self, event_type: str, pod, cluster_name: str):
        """Pod phase変更イベントの処理（クラスタ名を含む）"""
        pod_name = pod.metadata.name
        namespace = pod.metadata.namespace
        pod_uid = pod.metadata.uid
        current_phase = pod.status.phase if pod.status else "Unknown"

        # クラスタ名をログに含める
        self.logger.debug(f"[{cluster_name}] Pod event: {event_type} - {namespace}/{pod_name} - Phase: {current_phase}")

        # Phase変更の通知が必要かチェック
        pod_key = f"{cluster_name}:{pod_uid}"  # クラスタ名を含むキー
        should_notify = self._should_notify_phase_change(pod_key, current_phase)
        self.logger.debug(f"[{cluster_name}] Should notify phase change: {should_notify}")

        if should_notify:
            self.logger.debug(f"[{cluster_name}] Processing phase change for pod {namespace}/{pod_name} - {current_phase}")
            previous_phase = self.pod_phase_history.get(pod_key, {}).get("phase", "None")

            if previous_phase != "None":
                self.logger.info(f"[{cluster_name}] Pod phase change detected: {namespace}/{pod_name} - {previous_phase} → {current_phase}")
                # Pod phase データを抽出（クラスタ名を含む）
                phase_data = self._extract_pod_phase_data(pod, cluster_name)
                phase_data['cluster_name'] = cluster_name

                # ClusterAPIに通知
                success = self.clusterapi_client.pod_event(phase_data)

                if success:
                    self._update_phase_history(pod_key, current_phase)

                else:
                    self.logger.error(f"[{cluster_name}] Failed to notify ClusterAPI about phase change: {namespace}/{pod_name}")
            else:
                self.logger.debug(f"[{cluster_name}] Initial phase for {namespace}/{pod_name} - no previous phase to compare")
                # 初回のPhase通知は履歴に追加
                self._update_phase_history(pod_key, current_phase)

    def handle_pvc_event(self, event_type, pvc, cluster_name: str):
        # PVC削除イベントなどをここで処理
        self.logger.info(f"[{cluster_name}] PVC event: {event_type} - {pvc.metadata.namespace}/{pvc.metadata.name}")
        self.logger.debug(f"[{cluster_name}] PVC details: {pvc.to_dict()}")
        # 基本情報を取得
        pvc_data = {
            "metadata": {
                "name": pvc.metadata.name,
                "namespace": pvc.metadata.namespace,
                "uid": pvc.metadata.uid,
                "creation_timestamp": pvc.metadata.creation_timestamp.isoformat() if pvc.metadata.creation_timestamp else None
            },
            "status": pvc.status.to_dict() if pvc.status else {},
            "event_type": event_type,
            "cluster_name": cluster_name,
            "environment": self.environment,
            "event_timestamp": datetime.now().isoformat(),
        }
        if event_type == 'DELETED':
            # PVC削除イベントの処理
            self.logger.info(f"[{cluster_name}] PVC deleted: {pvc.metadata.name} in namespace {pvc.metadata.namespace}")
            # ClusterAPIに通知
            return self.clusterapi_client.pvc_event(pvc_data)
        elif event_type == 'ADDED' or event_type == 'MODIFIED':
            # PVC追加または変更イベントの処理
            self.logger.info(f"[{cluster_name}] PVC added/modified: {pvc.metadata.name} in namespace {pvc.metadata.namespace}")
        else:
            # その他のイベントタイプの処理
            self.logger.warning(f"[{cluster_name}] Unhandled PVC event type: {event_type}")
    
    def _extract_pod_phase_data(self, pod, cluster_name: str) -> Dict[str, Any]:
        """Pod phaseに特化したデータ抽出（クラスタ名を含む）"""
        current_phase = pod.status.phase if pod.status else "Unknown"

        # インスタンス情報の抽出
        instance_info = {
            "pod_name": pod.metadata.name,
            "namespace": pod.metadata.namespace,
            "uid": pod.metadata.uid,
            "node_name": pod.spec.node_name if pod.spec else None,
            "creation_timestamp": pod.metadata.creation_timestamp.isoformat() if pod.metadata.creation_timestamp else None,
            "cluster_name": cluster_name  # クラスタ名を追加
        }

        # Phase情報の詳細
        pod_key = f"{cluster_name}:{pod.metadata.uid}"
        phase_info = {
            "current_phase": current_phase,
            "previous_phase": self.pod_phase_history.get(pod_key, {}).get("phase"),
            "phase_change_timestamp": datetime.now().isoformat(),
            "cluster_name": cluster_name,
            "conditions": []
        }

        # コンテナの状態情報
        container_info = []
        if pod.status and pod.status.container_statuses:
            for cs in pod.status.container_statuses:
                container_info.append({
                    "name": cs.name,
                    "ready": cs.ready,
                    "restart_count": cs.restart_count,
                    "state": self._get_container_state(cs.state) if cs.state else "Unknown"
                })
        return {
            "instance": instance_info,
            "phase": phase_info,
            "containers": container_info,
            "environment": self.environment,
            "cluster_name": cluster_name,
            "event_timestamp": datetime.now().isoformat(),
        }
