import copy

from jsonschema import validate


class Config:
    """
    Class to make the app's config easily available throughout the program.

    A Config instance possesses a global config dictionary available to all
    instances, as well as a local one that can be used to provide default
    values, when they are not defined in the global dictionary.

    Variables stored in the config are meant to be constant throughout the
    program and updating them after initialization them should be avoided.
    """

    def __new__(cls, *args, **kwargs):
        if hasattr(cls, "_instance"):
            return cls._instance

        config = super(Config, cls).__new__(cls)
        config._global_config_dict = {}
        return config

    def __init__(self, local_config_dict=None):
        config_dict = copy.deepcopy(local_config_dict) or {}
        config_dict.update(self._global_config_dict)
        self._config_dict = config_dict

    @classmethod
    def new(cls, global_config_dict, exist_ok=False, schema=None):
        """
        Initialize a new global config.

        Raises AssertionError if a Config already exists, unles `exist_ok` is
        set to True.

        Validates the global_config_dict with jsonschema
        """
        assert not hasattr(cls, "_instance") or exist_ok, "Config has already been initialized"
        if schema:
            validate(instance=global_config_dict, schema=schema)

        if not hasattr(cls, "_instance"):
            cls._instance = super(Config, cls).__new__(cls)
        cls._instance._global_config_dict = copy.deepcopy(global_config_dict)

        return cls()

    @classmethod
    def clear(cls):
        """
        Clear existing config.
        """
        if hasattr(cls, "_instance"):
            del cls._instance

    def __getitem__(self, item):
        return self._config_dict[item]

    def get(self, item, default=None):
        return self._config_dict.get(item, default)

    def to_dict(self):
        """
        Returns config as dict
        """
        # Copy is returned to avoid accidentaly modifying the original config.
        return copy.deepcopy(self._config_dict)
