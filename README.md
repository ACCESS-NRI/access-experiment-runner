# access-experiment-runner

[![CI](https://github.com/ACCESS-NRI/access-experiment-runner/actions/workflows/ci.yml/badge.svg)](https://github.com/ACCESS-NRI/access-experiment-runner/actions/workflows/ci.yml)
[![CD](https://github.com/ACCESS-NRI/access-experiment-runner/actions/workflows/cd.yml/badge.svg)](https://github.com/ACCESS-NRI/access-experiment-runner/actions/workflows/cd.yml)
[![Coverage Status](https://codecov.io/gh/ACCESS-NRI/access-experiment-runner/branch/main/graph/badge.svg)](https://codecov.io/gh/ACCESS-NRI/access-experiment-runner)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue?style=flat-square)](https://opensource.org/license/apache-2-0)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

## About
The main role of the ACCESS experiment runner is to manage and monitor experiment job runs on the supercomputing environment (e.g., `Gadi`). It builds on `Payu`, handling the orchestration of multiple configuration branches, experiment setup, and job lifecycle.

## Key features
- Leverages `Payu` and run multiple experiments from different configuration branches.

- Submits and tracks PBS jobs on `Gadi`; oversees job lifecycle from submission through completion.
  - When a job completes within expected run times, the tool prints a confirmation and stops further submissions.
  - If a job fails, users may choose to inspect the working directory to diagnose the root cause. The tool will detect the failure and pause further actions, giving the user control over whether to resubmit.
  - Detects already running or queued jobs and avoids redundant submissions—quickly skips duplicates with a user notification.

## Installation
### User setup
The `experiment-runner` is installed in the `payu-dev` conda environment, hence loading `payu/dev` would directly make experiment-runner available for use.
```
module use /g/data/vk83/prerelease/modules && module load payu/dev
```

Alternatively, create and activate a python virtual environment, then install via pip,
```
python3 -m venv <path/to/venv> --system-site-packages
source <path/to/venv>/bin/activate

pip install experiment-runner
```

### Development setup
For contributors and developers, setup a development environment,
```
git clone https://github.com/ACCESS-NRI/access-experiment-runner.git
cd access-experiment-runner

# under a virtual environment
pip install -e .
```

## Usage
```
experiment-runner -i --help

usage: experiment-runner [-h] [-i INPUT_YAML_FILE]

Manage ACCESS experiments using configurable YAML input.
If no YAML file is specified, the tool will look for 'Experiment_runner.yaml' in the current directory.
If that file is missing, you must specify one with -i / --input-yaml-file.

options:
  -h, --help            show this help message and exit
  -i INPUT_YAML_FILE, --input-yaml-file INPUT_YAML_FILE
                        Path to the YAML file specifying parameter values for experiment runs.
                        Defaults to 'Experiment_runner.yaml' if present in the current directory.
```

One YAML example is provided in `example/Experiment_runner_example.yaml`

```yaml
test_path: /g/data/{PROJECT}/{USER}/prototype-0.1.0
repository_directory: 1deg_jra55_ryf
running_branches: [ctrl, perturb_1, perturb_2]
keep_uuid: True
nruns: [1,1,1]
```
where,

`test_path`: The base path to the experiment repository on the filesystem. In this case, it points to a prototype experiment runner checkout.

`repository_directory`: The specific experiment configuration directory inside test_path. Here it is the `1deg_jra55_ryf` setup.

`running_branches`: A list of git branches representing experiments to run.

`keep_uuid`: Preserve unique identifiers (UUIDs) across runs.

`nruns`: A list indicating how many runs to perform for each branch listed in running_branches.

## Workflow example
1. Trigger the experiment
```
experiment-runner -i example/Experiment_runner_example.yaml
```
2. The tool then checks status:
- Completed:
```
... already completed " {doneruns}, hence no new runs.
```
- Failed:
```
Clean up a failed job {work_dir} and prepare it for resubmission.
```
- Running/Queued: 
```
You have duplicated runs for in the same folder hence not submitting this job!
```