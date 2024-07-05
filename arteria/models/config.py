import copy

import yaml

DEFAULT_CONFIG_DICT = {

}

class Config:
    """
    Class to make the app's config easily available throughout the program.
    """

    _config_dict = DEFAULT_CONFIG_DICT

    def __new__(cls):
        if not hasattr(cls, "_instance"):
            cls._instance = super(Config, cls).__new__(cls)
        return cls._instance

    @classmethod
    def new(cls, config_dict):
        config = cls()
        config._config_dict = copy.deepcopy(config_dict)
        return config

    @classmethod
    def from_yaml(cls, path):
        """
        Load a config from a yaml file
        """
        # TODO add schema validation
        with open(path, 'r', encoding="utf-8") as config_file:
            config_dict = yaml.safe_load(config_file.read())

        return cls.new(config_dict)

    def __getitem__(self, item):
        return self._config_dict[item]

    def get(self, item, default=None):
        return self._config_dict.get(item, default)

    def to_dict(self):
        """
        Returns config as dict

        To avoid accidentaly modifying the original config, only a copy is
        returned.
        """
        return copy.deepcopy(self._config_dict)
