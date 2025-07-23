import threading
from typing import List, Dict
from kubernetes import client, config, watch
from kubernetes.client import Configuration

class MultiClusterPodPhaseWatcher:
    def __init__(self, environment: str = "development"):
        self.environment = environment
        self.config = self._load_environment_config()
        self.clusterapi_client = self._setup_clusterapi_client()
        self.watchers = []
        self._setup_logging()

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

            # Podの監視を開始
            stream = watch_instance.stream(v1_client.list_pod_for_all_namespaces)

            for event in stream:
                event_type = event['type']
                pod = event['object']

                # クラスタ名を含めてイベントを処理
                self.handle_pod_phase_event(event_type, pod, cluster_name)

        except Exception as e:
            self.logger.error(f"Error watching cluster {cluster_name}: {e}")
        finally:
            if 'watch_instance' in locals():
                watch_instance.stop()

    def _setup_cluster_client(self, cluster_config: Dict):
        """クラスタ固有のKubernetesクライアントを設定"""
        cluster_name = cluster_config.get('name')

        try:
            if 'config_file' in cluster_config:
                # kubeconfigファイルを使用
                config.load_kube_config(config_file=cluster_config['config_file'])
            elif 'host' in cluster_config:
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

        if event_type == 'DELETED':
            self.logger.warning(f"[{cluster_name}] Pod {namespace}/{pod_name} (UID: {pod_uid}) has been deleted")
            return

        # Phase変更の通知が必要かチェック
        pod_key = f"{cluster_name}:{pod_uid}"  # クラスタ名を含むキー
        should_notify = self._should_notify_phase_change(pod_key, current_phase)

        if should_notify:
            previous_phase = self.pod_phase_history.get(pod_key, {}).get("phase", "None")

            if previous_phase != "None":
                self.logger.info(f"[{cluster_name}] Pod phase change detected: {namespace}/{pod_name} - {previous_phase} → {current_phase}")

                # Pod phase データを抽出（クラスタ名を含む）
                phase_data = self._extract_pod_phase_data(pod, cluster_name)
                phase_data['event_type'] = event_type
                phase_data['cluster_name'] = cluster_name

                # ClusterAPIに通知
                success = self.clusterapi_client.update_pod_status(phase_data)

                if success:
                    self.logger.info(f"[{cluster_name}] Successfully notified ClusterAPI about phase change: {namespace}/{pod_name}")
                    self._update_phase_history(pod_key, current_phase)
                else:
                    self.logger.error(f"[{cluster_name}] Failed to notify ClusterAPI about phase change: {namespace}/{pod_name}")

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

        # 以下は既存のコードと同じ...

        return {
            "instance": instance_info,
            "phase": phase_info,
            "containers": container_info,
            "environment": self.environment,
            "cluster_name": cluster_name,
            "event_timestamp": datetime.now().isoformat()
        }
