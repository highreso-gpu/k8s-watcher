import threading
from kubernetes import client, config, watch
from kubernetes.client import Configuration
from kubernetes.config import ConfigException
import os
import logging
import time
from datetime import datetime
from typing import Dict, Any, Optional

import yaml
from .clusterapi_client import ClusterApiClient

class PodPhaseWatcher:
    def __init__(self, environment: str = "development"):
        self.environment = environment
        self.config = self._load_environment_config()
        self.clusterapi_client = self._setup_clusterapi_client()
        self.v1 = None
        self.watch = watch.Watch()
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
        self.logger.info(f"Starting pod phase k8s-watcher123 in {self.environment} environment")

    def _test_k8s_connection(self) -> bool:
        """Kubernetes API接続テスト"""
        try:
            self.logger.info("Testing connection to Kubernetes API...")

            # 接続テスト: Pod一覧を取得（最も確実な方法）
            try:
                # limit=1で最小限のリクエスト
                pods = self.v1.list_pod_for_all_namespaces(limit=1)
                self.logger.info(f"✅ Successfully connected to Kubernetes API - Found {len(pods.items)} pod(s)")

                # 環境別の追加情報
                if self.environment == 'development':
                    self.logger.info("development environment - connection test passed")

                    # 全Pod数を取得
                    try:
                        # all_pods = self.v1.list_pod_for_all_namespaces()
                        # total_pods = len(all_pods.items)
                        # self.logger.info(f"Total pods in cluster: {total_pods}")

                        # namespace一覧を取得
                        try:
                            namespaces = self.v1.list_namespace()
                            namespace_names = [ns.metadata.name for ns in namespaces.items]
                            self.logger.info(f"Available namespaces: {namespace_names}")
                        except Exception as ns_error:
                            self.logger.debug(f"Namespace list not available: {ns_error}")

                    except Exception as detail_error:
                        self.logger.debug(f"Could not get additional details: {detail_error}")
                else:
                    # 実際のK8s環境での追加チェック
                    try:
                        namespaces = self.v1.list_namespace()
                        namespace_names = [ns.metadata.name for ns in namespaces.items[:5]]
                        self.logger.info(f"Sample namespaces: {namespace_names}")
                    except Exception as e:
                        self.logger.debug(f"Failed to get namespace details: {e}")

                return True

            except Exception as pod_error:
                self.logger.error(f"Failed to connect to Kubernetes API: {pod_error}")

                # エラーの詳細を分析
                if "404" in str(pod_error):
                    self.logger.error("API endpoint not found - check if API server is running correctly")
                elif "connection" in str(pod_error).lower():
                    self.logger.error("Connection error - check if API server is accessible")
                elif "unauthorized" in str(pod_error).lower():
                    self.logger.error("Authentication error - check if token is valid")

                return False

        except Exception as e:
            self.logger.error(f"Connection test error: {e}")
            return False

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

    def _is_critical_phase(self, phase: str) -> bool:
        """クリティカルなPhaseかどうかを判定"""
        return phase in ["Failed", "Unknown"]

    def handle_pod_phase_event(self, event_type: str, pod):
        """Pod phase変更イベントの処理"""
        pod_name = pod.metadata.name
        namespace = pod.metadata.namespace
        pod_uid = pod.metadata.uid
        self.logger.debug(f"Handling pod phase event: {event_type} - {namespace}/{pod_name} (UID: {pod_uid})")
        current_phase = pod.status.phase if pod.status else "Unknown"

        self.logger.debug(f"Pod event: {event_type} - {namespace}/{pod_name} - Phase: {current_phase}")

        # 監視対象のnamespaceかチェック
        # target_namespaces = self.config.get('watcher', {}).get('namespaces', [])
        # logging.debug(f"Target namespaces: {target_namespaces}")
        # if target_namespaces and namespace not in target_namespaces:
        #     self.logger.debug(f"Skipping pod {namespace}/{pod_name} - not in target namespaces")
        #     return

        # Phase変更の通知が必要かチェック
        should_notify = self._should_notify_phase_change(pod_uid, current_phase)

        previous_phase = self.pod_phase_history.get(pod_uid, {}).get("phase", "None")

        if should_notify:
            # ログレベルの決定（Critical phaseの場合はWARNING）
            log_level = logging.WARNING if self._is_critical_phase(current_phase) else logging.INFO
            self.logger.log(log_level, 
                f"Pod phase change detected: {namespace}/{pod_name} - {previous_phase} → {current_phase}")
            # # 履歴を更新 (not here to avoid duplicate updates)
            # self._update_phase_history(pod_uid, current_phase)
            if previous_phase != "None":
                # Pod phase データを抽出
                phase_data = self._extract_pod_phase_data(pod)
                phase_data['event_type'] = event_type

                self.logger.debug(f"Extracted phase data: {phase_data}")

                # ClusterAPIに通知
                success = self.clusterapi_client.update_pod_status(phase_data)

                if success:
                    self.logger.info(f"Successfully notified ClusterAPI about phase change: {namespace}/{pod_name}")
                    # 履歴を更新
                    self._update_phase_history(pod_uid, current_phase)
                else:
                    self.logger.error(f"Failed to notify ClusterAPI about phase change: {namespace}/{pod_name}")
            else:
                self.logger.debug(f"Initial phase for {namespace}/{pod_name} - no previous phase to compare")
                # 初回のPhase通知は履歴に追加
                self._update_phase_history(pod_uid, current_phase)
        else:
            self.logger.debug(f"No phase change for {namespace}/{pod_name} - skipping notification")

    def handle_pvc_event(self, event_type, pvc):
        # PVC削除イベントなどをここで処理
        self.logger.info(f"PVC event: {event_type} - {pvc.metadata.namespace}/{pvc.metadata.name}")
        # 必要に応じてClusterAPI等へ通知

    def setup_k8s_client(self) -> bool:
        """Kubernetesクライアントのセットアップ（APIサーバーホスト指定対応）"""
        try:
            k8s_config = self.config.get('kubernetes', {})

            # APIサーバーのホストが直接指定されている場合
            if 'host' in k8s_config:
                api_host = k8s_config['host']
                verify_ssl = k8s_config.get('verify_ssl', True)
                token = k8s_config.get('token')

                self.logger.info(f"Connecting to Kubernetes API server at: {api_host}")

                # カスタム設定を作成
                configuration = Configuration()
                configuration.host = api_host
                configuration.verify_ssl = verify_ssl

                # 認証トークンの設定
                if token:
                    configuration.api_key = {"authorization": f"Bearer {token}"}
                    self.logger.info("Using token authentication")
                else:
                    self.logger.warning("No authentication token provided")

                # デフォルト設定として適用
                client.Configuration.set_default(configuration)

            elif k8s_config.get('use_incluster_config', False):
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
            return self._test_k8s_connection()

        except ConfigException as e:
            self.logger.error(f"Kubernetes config error: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Error setting up k8s client: {e}")
            return False

    def start_watching(self):
        """Pod phase監視を開始"""
        if not self.setup_k8s_client():
            self.logger.error("Failed to setup Kubernetes client")
            return

        # clusterapiの疎通確認
        if not self.clusterapi_client.health_check():
            self.logger.warning("ClusterAPI health check failed, but continuing...")
        else:
            self.logger.info("ClusterAPI health check passed")

        self.logger.info(f"Starting Pod Phase watcher in {self.environment} environment...")
        target_namespaces = self.config.get('watcher', {}).get('namespaces', [])
        if target_namespaces:
            self.logger.info(f"Monitoring namespaces: {target_namespaces}")
        else:
            self.logger.info("Monitoring all namespaces")

        # self.logger.info("Monitoring Pod phases: Pending, Running, Succeeded, Failed, Unknown")

        try:
            # Podの監視を開始
            pod_stream = self.watch.stream(self.v1.list_pod_for_all_namespaces)

            # PVCの監視
            pvc_stream = self.watch.stream(self.v1.list_persistent_volume_claim_for_all_namespaces)

            # for event in stream:
            #     # logging.debug(f"Received event: {event}")
            #     event_type = event['type']
            #     pod = event['object']
            #     self.handle_pod_phase_event(event_type, pod)
            def watch_pods():
                for event in pod_stream:
                    event_type = event['type']
                    pod = event['object']
                    self.handle_pod_phase_event(event_type, pod)
            
            def watch_pvcs():
                for event in pvc_stream:
                    event_type = event['type']
                    pvc = event['object']
                    self.handle_pvc_event(event_type, pvc)
            pod_thread = threading.Thread(target=watch_pods)
            pvc_thread = threading.Thread(target=watch_pvcs)
            pod_thread.start()
            pvc_thread.start()
            pod_thread.join()
            pvc_thread.join()

        except KeyboardInterrupt:
            self.logger.info("Stopping Pod Phase watcher...")
        except Exception as e:
            self.logger.error(f"Error in Pod Phase watcher: {e}")
            raise
        finally:
            self.watch.stop()
