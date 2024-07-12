import pytest
import tempfile
import yaml

from arteria.models.config import Config


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
    # TODO test `exist_ok`
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
    # TODO add comments to explain expected behavior
    assert Config().to_dict() == config_dict

    assert Config() == Config()

    new_config = {"abc": "123"}

    with pytest.raises(AssertionError):
        Config.new(new_config)

    Config.new(new_config, exist_ok=True)
    assert config.to_dict() == new_config
    assert Config().to_dict() == new_config


def test_default_config_from_scratch():
    # TODO add comments to explain expected behavior
    defaults = {"default_variable": 1}
    config = Config(defaults)
    assert config.to_dict() == defaults
    assert config["default_variable"] == defaults["default_variable"]

    defaults["default_variable"] = 0
    assert config["default_variable"] != defaults["default_variable"]


def test_default_config_from_existing_config(config, config_dict):
    # TODO add comments to explain expected behavior
    defaults = {"default_variable": 1, "port": 0}
    config = Config(defaults)

    expected_dict = {k: v for k, v in defaults.items()}
    expected_dict.update(config_dict)

    assert config.to_dict() == expected_dict

    config = Config() # issue, still contains default variable
    assert config.to_dict() == config_dict
