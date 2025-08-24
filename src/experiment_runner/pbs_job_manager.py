"""
PBS Job Management for the experiment management

This module provides utilities for managing PBS jobs, including:
- Checking for existing jobs.
- Handling duplicate submissions.
- Committing changes before execution.
- Starting new PBS runs.
"""

from pathlib import Path
import subprocess

GADI_PREFIX = "gadi.nci.org.au:"


def output_existing_pbs_jobs() -> dict:
    """
    Retrieves and parses the current PBS job status using the `qstat -f` command.

    Returns:
        dict: A dictionary containing PBS job information, where each key
                is a job ID, and the value is a dictionary of job attributes.
    Raises:
        RuntimeError: If the PBS job status command fails.
    """
    current_job_status_path = Path("current_job_status")
    command = f"qstat -f > {current_job_status_path}"
    subprocess.run(command, shell=True, check=True)

    pbs_jobs = {}
    current_key = None
    current_value = ""
    job_id = None

    with open(current_job_status_path, "r", encoding="utf-8") as f:
        pbs_job_file = f.read()

    def _flush_pair():
        nonlocal current_key, current_value, job_id
        if current_key and job_id:
            pbs_jobs[job_id][current_key] = current_value.strip()
            current_key = None
            current_value = ""

    pbs_job_file = pbs_job_file.replace("\t", "        ")

    for line in pbs_job_file.splitlines():
        line = line.rstrip()
        if not line:
            _flush_pair()
            continue

        if line.startswith("Job Id:"):
            _flush_pair()
            job_id = line.split(":", 1)[1].strip()
            pbs_jobs[job_id] = {}
            continue

        if line.startswith("        ") and current_key:  # 8 indents multi-line
            current_value += line.strip()
            continue

        if line.startswith("    ") and " = " in line:  # 4 indents for new pair
            # Save the previous multi-line value
            _flush_pair()
            key, value = line.split(" = ", 1)  # save key
            current_key = key.strip()
            current_value = value.strip()
            continue

    # end of file, flush last pair
    _flush_pair()

    # Clean up the temporary file: `current_job_status`
    current_job_status_path.unlink()
    return pbs_jobs


def _extract_current_and_parent_path(tmp_path: str) -> tuple[Path, Path]:
    """
    Extracts the current (experiment) and parent (test) paths from a PBS job error path.

    Args:
        tmp_path (str): The error path from a PBS job.

    Returns:
        tuple: (experiment folder path, parent test folder path).
    """
    tmp_path = Path(tmp_path)
    folder_path = tmp_path.parent
    folder_str = str(folder_path)
    if folder_str.startswith(GADI_PREFIX):
        folder_str = folder_str[len(GADI_PREFIX) :]
    folder_path = Path(folder_str)
    parent_path = folder_path.parent

    return folder_path, parent_path


class PBSJobManager:
    """
    Manages PBS jobs by checking for existing jobs, handling duplicates,
    committing changes to the repo, and starting new experiment runs.
    """

    def pbs_job_runs(self, path: Path, nruns: int) -> None:
        """
        Manages PBS job runs by checking for existing jobs, handling duplicates,
        committing changes, and starting experiment runs if no duplication is detected.

        Args:
            path (Path): Path to the experiment directory.
        """
        # check existing pbs jobs
        pbs_jobs = output_existing_pbs_jobs()

        # check for duplicated running jobs
        duplicated_bool = self._check_duplicated_jobs(path, pbs_jobs)

        # start control runs, count existing runs and do additional runs if needed
        self._start_experiment_runs(path, nruns, duplicated_bool)

    def _check_duplicated_jobs(self, path: Path, pbs_jobs: dict) -> bool:
        """
        Checks for duplicate running jobs in the same parent folder.

        Args:
            path (Path): Path to the experiment directory.
            pbs_jobs (dict): Dictionary of current PBS jobs.

        Returns:
            bool: True if duplicate jobs are detected, otherwise False.
        """
        parent_paths = {}
        duplicated = False

        for _, job_info in pbs_jobs.items():
            folder_path, parent_path = _extract_current_and_parent_path(
                job_info["Error_Path"]
            )
            job_state = job_info["job_state"]
            if job_state not in ("F", "S"):
                if parent_path not in parent_paths:
                    parent_paths[parent_path] = []
                parent_paths[parent_path].append(folder_path)

        for parent_path, folder_paths in parent_paths.items():
            if path in folder_paths:
                print(
                    f"-- You have duplicated runs for '{Path(*path.parts[-2:])}'"
                    f"in the same folder '{parent_path}', "
                    "hence not submitting this job!\n"
                )
                duplicated = True

        return duplicated

    def _start_experiment_runs(self, path: Path, nruns: int, duplicated: bool) -> None:
        """
        Starts the experiment runs if no duplicate jobs are detected.

        Args:
            path (Path): Path to the experiment directory.
            duplicated (bool): Indicates whether duplicate jobs were found.
        """
        if duplicated:
            return

        # first clean `work` directory for failed jobs
        self._clean_workspace(path)

        doneruns = len(list(Path(path, "archive").glob("output[0-9][0-9][0-9]*")))
        newruns = nruns - doneruns
        if newruns > 0:
            print(f"\nStarting {newruns} new experiment runs\n")
            command = f"cd {path} && payu run -n {newruns} -f"
            subprocess.run(command, shell=True, check=True)
            print("\n")
        else:
            print(
                f"-- `{Path(*path.parts[-2:])}` already completed "
                f"{doneruns} runs, hence no new runs.\n"
            )

    def _clean_workspace(self, path: Path) -> None:
        """
        Cleans `work` directory for failed jobs.

        Args:
            path (Path): Path to the experiment directory.
        """
        work_dir = path / "work"
        # in case any failed job
        if work_dir.is_symlink() and work_dir.is_dir():
            # Payu sweep && setup to ensure the changes correctly && remove the `work` directory
            command = "payu sweep && payu setup"
            subprocess.run(command, shell=True, check=False)
            print(f"Clean up a failed job {work_dir} and prepare it for resubmission.")
