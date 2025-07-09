from kubernetes import client, config
import os
import json

def test_k8s_mock():
    print("Testing k8s mock connection...")

    try:
        # kubeconfigを読み込み
        kubeconfig_path = "./assets/config"
        print(f"Loading kubeconfig from: {kubeconfig_path}")

        if not os.path.exists(kubeconfig_path):
            print(f"❌ Kubeconfig file not found: {kubeconfig_path}")
            return False

        config.load_kube_config(config_file=kubeconfig_path)
        print("✅ Kubeconfig loaded successfully")

        # APIクライアントを作成
        v1 = client.CoreV1Api()
        print("✅ CoreV1Api client created")

        # k8s mockサーバーの情報を表示
        try:
            # 設定情報を取得
            contexts, active_context = config.list_kube_config_contexts(config_file=kubeconfig_path)
            if active_context:
                server_url = active_context.get('context', {}).get('cluster')
                print(f"📡 Connecting to: {server_url}")
        except Exception as e:
            print(f"⚠️  Could not get server info: {e}")

        # 接続テスト1: Pod一覧（最も基本的なテスト）
        print("\n🔍 Testing Pod API...")
        try:
            pods = v1.list_pod_for_all_namespaces(limit=5)
            print(f"✅ Pod API works - Found {len(pods.items)} pod(s)")

            # Pod詳細を表示
            for i, pod in enumerate(pods.items):
                print(f"   Pod {i+1}: {pod.metadata.namespace}/{pod.metadata.name} - {pod.status.phase}")

        except Exception as e:
            print(f"❌ Pod API failed: {e}")
            return False

        # 接続テスト2: Namespace一覧
        print("\n🔍 Testing Namespace API...")
        try:
            namespaces = v1.list_namespace()
            print(f"✅ Namespace API works - Found {len(namespaces.items)} namespace(s)")

            for ns in namespaces.items:
                print(f"   Namespace: {ns.metadata.name}")

        except Exception as e:
            print(f"⚠️  Namespace API failed (may not be implemented in mock): {e}")

        # 接続テスト3: Watch APIのテスト
        print("\n🔍 Testing Watch API...")
        try:
            from kubernetes import watch
            w = watch.Watch()

            # 短時間だけwatchをテスト
            print("   Starting watch test (5 seconds)...")
            import time
            start_time = time.time()
            event_count = 0

            for event in w.stream(v1.list_pod_for_all_namespaces, timeout_seconds=5):
                event_count += 1
                print(f"   Watch event {event_count}: {event['type']} - {event['object'].metadata.name}")

                # 5秒経過またはイベント5個で終了
                if time.time() - start_time > 5 or event_count >= 5:
                    break

            w.stop()
            print(f"✅ Watch API works - Received {event_count} events")

        except Exception as e:
            print(f"⚠️  Watch API test failed: {e}")

        print("\n🎉 k8s mock connection test completed!")
        return True

    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        print(f"Error type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        return False

def check_kubeconfig():
    """kubeconfigファイルの内容を確認"""
    kubeconfig_path = "./assets/config"

    if not os.path.exists(kubeconfig_path):
        print(f"❌ Kubeconfig not found: {kubeconfig_path}")
        return

    try:
        print(f"\n📄 Kubeconfig content preview:")
        with open(kubeconfig_path, 'r') as f:
            content = f.read()
            lines = content.split('\n')
            for i, line in enumerate(lines[:20]):  # 最初の20行だけ表示
                print(f"   {i+1:2d}: {line}")
            if len(lines) > 20:
                print(f"   ... ({len(lines) - 20} more lines)")
    except Exception as e:
        print(f"⚠️  Could not read kubeconfig: {e}")

if __name__ == "__main__":
    check_kubeconfig()
    test_k8s_mock()
