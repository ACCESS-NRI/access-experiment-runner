from pathlib import Path
from payu.branch import clone, list_branches
from .base_experiment import BaseExperiment
from .pbs_job_manager import PBSJobManager
import git


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

        for indx, (expt, branch, nrun) in enumerate(zip(all_cloned_directories, self.running_branches, self.nruns)):
            self._generate_restart_symlinks_for_branch(expt, branch, indx)
            self.pbsjobmanager.pbs_job_runs(expt, nrun)

    def _print_branches_available(self):
        list_branches(config_path=self.base_directory / "config.yaml")

    def _create_cloned_directory(self) -> None:
        """
        Clones the experiment repository if it doesn't already exist.
        """
        if not self.running_branches:
            raise ValueError("No running branches provided!")

        all_cloned_directories = [Path(self.test_path) / b / self.repository_directory for b in self.running_branches]

        for clone_dir, branch in zip(all_cloned_directories, self.running_branches):
            if clone_dir.exists():
                print(f"-- Test dir: {clone_dir} already exists, skipping cloning.")
                if not self._update_existing_repo(clone_dir, branch):
                    print(f"Failed to update existing repo {clone_dir}, leaving as it is.")
            else:
                print(f"-- Cloning branch '{branch}' into {clone_dir}...")
                self._do_clone(clone_dir, branch)

        return all_cloned_directories

    def _do_clone(self, clone_dir: Path, branch: str):
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

    def _update_existing_repo(self, clone_dir: Path, target_ref: str) -> bool:
        """
        Updates the repo without removing the dir or untracked files
        target_ref: branch to checkout
        """

        try:
            repo = git.Repo(str(clone_dir))
            remote = repo.remotes.origin
            remote.fetch(prune=True)

            # save current HEAD
            current_commit = repo.head.commit.hexsha

            # ensure branch exists
            if target_ref in repo.heads:
                repo.git.checkout(target_ref)
            else:
                repo.git.checkout("-b", target_ref, f"origin/{target_ref}")

            # try pulling with rebase
            try:
                repo.git.pull("--rebase", "--autostash", "origin", target_ref)
            except git.exc.GitCommandError:
                repo.git.reset("--keep", f"origin/{target_ref}")

            # save new HEAD after update
            new_commit = repo.head.commit.hexsha

            rel_path = clone_dir.relative_to(self.test_path)

            if current_commit == new_commit:
                print(f"-- Repo {rel_path} is already up to date with {target_ref}.")
            else:
                print(
                    f"-- Repo {rel_path} updated from {current_commit[:7]} to {new_commit[:7]} on branch {target_ref}."
                )
                changed = repo.git.diff("--name-only", current_commit, new_commit).splitlines()
                if changed:
                    print("-- Changed files:")
                    for file in changed:
                        print(f"   -- {file}")

            return True
        except git.exc.GitCommandError as e:
            print(f"Failed updating existing repo {rel_path}: {e}")
            return False

    def _generate_restart_symlinks_for_branch(self, expt_path: Path, branch: str, indx: int) -> None:
        """
        Create symlink:
        <branch>/<repo>/archive/<restart_tag>  ->  <src_branch>/<repo>/archive/<restart_tag>
        when mode == 'restart'. For 'cold' do nothing.
        """
        restart_mode, src_branch, restart_tag = self._resolve_restart_tag(branch, indx)

        if restart_mode == "cold":
            print(f"-- {branch} Starting from cold start.")
            return

        # restart dir
        source_restart_path = (
            Path(self.test_path) / src_branch / self.repository_directory / "archive" / restart_tag
        ).resolve()

        # check if source restart path exists and contains restart files
        if not source_restart_path.exists() or not source_restart_path.is_dir():
            raise FileNotFoundError(f"-- Source restart path does not exist: {source_restart_path}")

        if not any(source_restart_path.iterdir()):
            raise FileNotFoundError(f"-- Source restart path is empty in {source_restart_path}")

        # restart dir symlink for this branch
        dest_restart_path = expt_path / "archive" / restart_tag

        needs_update = not dest_restart_path.is_symlink() or (
            dest_restart_path.is_symlink() and not dest_restart_path.exists()
        )

        if needs_update:
            if dest_restart_path.exists() or dest_restart_path.is_symlink():
                dest_restart_path.unlink()
                print(f"-- Removed old restart symlink: {dest_restart_path}")
            dest_restart_path.symlink_to(source_restart_path)
            print(f"-- Created restart symlink: {dest_restart_path} -> {source_restart_path}")

        else:
            print(f"-- Restart symlink already exists and is valid: {dest_restart_path}")

        return source_restart_path

    def _resolve_restart_tag(self, branch: str, indx: int) -> str:
        """
        Return the restart tag to a per-branch (restart_mode, src_branch, restart_tag).
        """
        s = self.startfrom_restart
        if isinstance(s, list):
            if len(s) != len(self.running_branches):
                raise ValueError("startfrom_restart list length must match running_branches.")
            return self._parse_restart_entry(s[indx])
        else:
            raise TypeError("startfrom_restart must be list[str, str, str]!")

    def _parse_restart_entry(self, entry: str) -> tuple[str, str, str]:
        """
        Returns a tuple (restart_mode, src_branch, restart_tag)
        restart_mode: 'cold' | 'restart'
        src_branch: branch name providing the restart files
        restart_tag: restartxxx, suchas restart000
        """
        if entry == "cold":
            return ("cold", None, None)

        src_branch, restart_tag = entry.split("/", 1)
        return ("restart", src_branch, restart_tag)
