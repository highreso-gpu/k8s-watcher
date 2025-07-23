from watcher.multi_cluster_pod_phase_watcher import MultiClusterPodPhaseWatcher
# from watcher.pod_phase_watcher import PodPhaseWatcher

import sys
import os

def main():
    # 環境を決定（環境変数 > コマンドライン引数 > デフォルト）
    environment = os.getenv('ENVIRONMENT', 'development')  # デフォルトは'development'

    if len(sys.argv) > 1:
        environment = sys.argv[1]

    # サポートされている環境かチェック
    supported_environments = ['development', 'staging', 'production']
    if environment not in supported_environments:
        print(f"Error: Unsupported environment '{environment}'")
        print(f"Supported environments: {supported_environments}")
        sys.exit(1)

    print(f"Starting k8s-watcher in '{environment}' environment")

    # Pod Watcherを作成・開始
    try:
        # ここでPodPhaseWatcherを使う場合はコメントアウトを外してください
        # watcher = PodPhaseWatcher(environment=environment)
        # watcher.start_watching()
        watcher = MultiClusterPodPhaseWatcher(environment=environment)
        watcher.start_watching_all_clusters()
    except Exception as e:
        print(f"Error starting watchers: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
