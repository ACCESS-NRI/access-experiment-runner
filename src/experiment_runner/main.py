import argparse
import os

from .utils import read_yaml
from .experiment_runner import ExperimentRunner


def main():
    """
    Managing ACCESS experiment runs.

    This script loads experiment configurations from a YAML file
    and invokes the ExperimentRunner to produce the required setups.

    Command-line Arguments:
        -i, --input-yaml-file (str, optional):
            Path to the YAML file specifying parameter values for the experiment runs.
            Defaults to 'Experiment_runner.yaml' if it exists.
    """

    parser = argparse.ArgumentParser(
        description=(
            "Manage ACCESS experiments using configurable YAML input.\n"
            "If no YAML file is specified, the tool will look for 'Experiment_runner.yaml' "
            "in the current directory.\n"
            "If that file is missing, you must specify one with -i / --input-yaml-file."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )

    parser.add_argument(
        "-i",
        "--input-yaml-file",
        type=str,
        help=(
            "Path to the YAML file specifying parameter values for experiment runs.\n"
            "Defaults to 'Experiment_runner.yaml' if present in the current directory."
        ),
    )

    args = parser.parse_args()
    if args.input_yaml_file:
        input_yaml = args.input_yaml_file
    elif os.path.exists("Experiment_runner.yaml"):
        input_yaml = "Experiment_runner.yaml"
    else:
        parser.error(
            "No YAML file specified and 'Experiment_runner.yaml' not found.\n"
            "Please provide one using -i / --input-yaml-file."
        )

    # Load the YAML file
    indata = read_yaml(input_yaml)

    # Run the experiment runner
    runner = ExperimentRunner(indata)
    runner.run()
