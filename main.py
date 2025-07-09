from watcher.pod_watcher import PodWatcher
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
        watcher = PodWatcher(environment=environment)
        watcher.start_watching()
    except Exception as e:
        print(f"Error starting watcher: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
