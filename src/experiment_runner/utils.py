from pathlib import Path
from ruamel.yaml import YAML

ryaml = YAML()
ryaml.preserve_quotes = True


def read_yaml(yaml_path: Path) -> dict:
    """
    Reads a YAML file and returns a dictionary.

    Args:
        yaml_path (str): The path to the YAML file.

    Returns:
        dict: Parsed YAML content.
    """
    yaml_path = Path(yaml_path)

    with yaml_path.open("r", encoding="utf-8") as f:
        return ryaml.load(f)


def write_yaml(data: dict, yaml_path: Path) -> None:
    """
    Writes a dictionary to a YAML file while preserving formatting.

    Args:
        data (dict): The dictionary containing YAML data.
        yaml_path (str): The path to save the YAML file.
    """
    yaml_path = Path(yaml_path)
    with yaml_path.open("w", encoding="utf-8") as f:
        ryaml.dump(data, f)
