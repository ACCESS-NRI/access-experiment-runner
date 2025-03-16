from pathlib import Path
from payu.branch import clone, list_branches
from .base_experiment import BaseExperiment
from .pbs_job_manager import PBSJobManager


class ExperimentRunner(BaseExperiment):
    """
    Handles setup, cloning, and running control & perturbation experiments.
    """

    def __init__(self, indata: dict):
        super().__init__(indata)

        self.pbsjobmanager = PBSJobManager()

    def run(self) -> None:
        """
        Main function to set up and run experiments.
        """
        self._print_branches_available()
        all_cloned_directories = self._create_cloned_directory()

        for expt, nrun in zip(all_cloned_directories, self.nruns):
            self.pbsjobmanager.pbs_job_runs(expt, nrun)

    def _print_branches_available(self):
        list_branches(config_path=self.base_directory / "config.yaml")

    def _create_cloned_directory(self) -> None:
        """
        Clones the experiment repository if it doesn't already exist.
        """
        if not self.running_branches:
            raise ValueError("No running branches provided!")

        all_cloned_directories = [
            Path(self.test_path) / b / self.repository_directory
            for b in self.running_branches
        ]

        for clone_dir, branch in zip(all_cloned_directories, self.running_branches):
            if clone_dir.exists():
                print(f"-- Test dir: {clone_dir} already exists, skipping cloning.")
            else:
                print(f"-- Cloning branch '{branch}' into {clone_dir}...")
                clone(
                    repository=self.base_directory,
                    directory=clone_dir,
                    branch=branch,
                    keep_uuid=self.keep_uuid,
                    model_type=self.model_type,
                    config_path=self.config_path,
                    lab_path=self.lab_path,
                    new_branch_name=self.new_branch_name,
                    restart_path=self.restart_path,
                    parent_experiment=self.parent_experiment,
                    start_point=self.start_point,
                )
        return all_cloned_directories
