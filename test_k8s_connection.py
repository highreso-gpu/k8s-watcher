from kubernetes import client, config
import os

def test_k8s_connection():
    print("Testing Kubernetes connection...")

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

        # 利用可能なメソッドを確認
        print("\nAvailable methods in CoreV1Api:")
        methods = [method for method in dir(v1) if not method.startswith('_')]
        for method in methods[:10]:  # 最初の10個だけ表示
            print(f"  - {method}")
        print("  ...")

        # 接続テスト1: バージョン情報
        try:
            version_api = client.VersionApi()
            version_info = version_api.get_code()
            print(f"✅ Server version: {version_info.git_version}")
        except Exception as e:
            print(f"⚠️  Version API failed: {e}")

        # 接続テスト2: namespace一覧
        try:
            namespaces = v1.list_namespace(limit=1)
            print(f"✅ Namespace list: {len(namespaces.items)} items")
        except Exception as e:
            print(f"❌ Namespace list failed: {e}")
            return False

        # 接続テスト3: Pod一覧
        try:
            pods = v1.list_pod_for_all_namespaces(limit=1)
            print(f"✅ Pod list: {len(pods.items)} items")
        except Exception as e:
            print(f"❌ Pod list failed: {e}")
            return False

        print("\n🎉 All connection tests passed!")
        return True

    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        print(f"Error type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_k8s_connection()
