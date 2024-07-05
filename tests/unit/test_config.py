import pytest
import tempfile
import yaml

from arteria.models.config import Config, DEFAULT_CONFIG_DICT


@pytest.fixture()
def config_dict():
    return {
        "port": 8080,
        "grace_minutes": 10,
        "monitored_paths": ["/tmp/path1", "/tmp/path2"],
    }


@pytest.fixture()
def config(config_dict):
    config_object = Config.new(config_dict)

    yield config_object

    del Config._instance


def test_config_from_yaml(config_dict):
    with tempfile.NamedTemporaryFile(mode="r+") as config_file:
        config_file.write(yaml.dump(config_dict))
        config_file.seek(0)
        config = Config.from_yaml(config_file.name)

        assert config._config_dict == config_dict

        del Config._instance


def test_config_to_dict(config, config_dict):
    assert config.to_dict() == config_dict


def test_config_getitem(config, config_dict):
    for key, value in config_dict.items():
        assert config[key] == value


def test_config_get(config, config_dict):
    for key, value in config_dict.items():
        assert config.get(key) == value

    assert config.get("port", "123") == config_dict["port"]
    assert config.get("new_key", "123") == "123"


def test_config_immutable(config, config_dict):
    with pytest.raises(TypeError):
        config["url"] = "http://fakeurl.com"

    with pytest.raises(TypeError):
        config["port"] = "1337"

    config.to_dict()["port"] = 0
    assert config["port"] == config_dict["port"]


def test_config_new(config, config_dict):
    assert Config().to_dict() == config_dict

    assert Config() == Config()

    new_config = {"abc": "123"}
    Config.new(new_config)
    assert config.to_dict() == new_config
    assert Config().to_dict() == new_config


def test_default_config():
    assert Config().to_dict() == DEFAULT_CONFIG_DICT
