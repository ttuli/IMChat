import os
import sys
import unittest
from unittest.mock import MagicMock, patch

# Find project root (where etc/ is relative to)
def find_root(start_dir):
    curr = start_dir
    while curr != os.path.dirname(curr):
        if os.path.exists(os.path.join(curr, "pyproject.toml")):
            return curr
        curr = os.path.dirname(curr)
    return None

root = find_root(os.path.abspath(os.path.dirname(__file__)))
if root:
    sys.path.append(root)
else:
    print("Warning: Project root not found")

from internal.apps.Llm.python.core.config_loader import expand_env, expand_env_recursive, ConfigLoader, LocalConfig
from internal.apps.Llm.python.core.parsers import FileParser, NacosParser
from internal.apps.Llm.python.core.config import AppConfig

class TestConfigLoader(unittest.TestCase):
    def test_expand_env(self):
        os.environ["TEST_VAR"] = "hello"
        self.assertEqual(expand_env("${TEST_VAR}"), "hello")
        self.assertEqual(expand_env("$TEST_VAR"), "hello")
        self.assertEqual(expand_env("prefix_${TEST_VAR}_suffix"), "prefix_hello_suffix")
        self.assertEqual(expand_env("${UNDEFINED_VAR}"), "${UNDEFINED_VAR}")

    def test_expand_env_recursive(self):
        os.environ["VAR1"] = "val1"
        os.environ["VAR2"] = "val2"
        data = {
            "key1": "${VAR1}",
            "key2": ["${VAR2}", "const"],
            "key3": {"subkey": "$VAR1"}
        }
        expanded = expand_env_recursive(data)
        self.assertEqual(expanded["key1"], "val1")
        self.assertEqual(expanded["key2"], ["val2", "const"])
        self.assertEqual(expanded["key3"]["subkey"], "val1")

    @patch("yaml.safe_load")
    @patch("builtins.open")
    @patch("os.path.exists")
    def test_load_local_config(self, mock_exists, mock_open, mock_yaml):
        mock_exists.return_value = True
        mock_yaml.return_value = {
            "config_source": "file",
            "file": {"path": "etc/app.yaml"}
        }
        
        loader = ConfigLoader("dummy.yaml")
        local_cfg = loader.load_local_config()
        
        self.assertEqual(local_cfg.config_source, "file")
        self.assertEqual(local_cfg.file.path, "etc/app.yaml")

    @patch("internal.apps.Llm.python.core.parsers.FileParser.load")
    @patch("internal.apps.Llm.python.core.config_loader.ConfigLoader.load_local_config")
    def test_loader_dispatch_file(self, mock_load_local, mock_file_parser_load):
        mock_load_local.return_value = LocalConfig(
            config_source="file", 
            file={"path": "etc/app.yaml"}
        )
        
        loader = ConfigLoader("dummy.yaml")
        loader.local_config = mock_load_local.return_value
        
        loader.load(AppConfig)
        mock_file_parser_load.assert_called_once_with(AppConfig)

    @patch("nacos.NacosClient")
    @patch("yaml.safe_load")
    def test_nacos_parser(self, mock_yaml, mock_nacos):
        mock_client = MagicMock()
        mock_nacos.return_value = mock_client
        mock_client.get_config.return_value = "key: value"
        mock_yaml.return_value = {
            "SuggestLlmApiKey": "secret",
            "SuggestLlmUrl": "http://api.com",
            "SuggestLlmModel": "gpt-4"
        }
        
        nacos_cfg = MagicMock()
        nacos_cfg.host = "localhost"
        nacos_cfg.namespace = "ns"
        nacos_cfg.user = "u"
        nacos_cfg.password = "p"
        nacos_cfg.dataid = "did"
        nacos_cfg.group = "g"
        nacos_cfg.timeout_ms = 5000
        
        parser = NacosParser(nacos_cfg)
        app_cfg = parser.load(AppConfig)
        
        self.assertEqual(app_cfg.suggest_llm_config.api_key, "secret")
        self.assertEqual(app_cfg.suggest_llm_config.url, "http://api.com")

if __name__ == "__main__":
    unittest.main()
