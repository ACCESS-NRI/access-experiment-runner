"""
Tests for running and deleting experiments that the experiment generator made as git worktrees
(`use_worktrees: true`), with a real git repository.
"""

import subprocess
from pathlib import Path

import git
import pytest

import experiment_runner.experiment_runner as exp_runner

REPO_DIR = "test_repo"


def init_repo(path: Path) -> git.Repo:
    repo = git.Repo.init(path, initial_branch="main")
    with repo.config_writer() as config:
        config.set_value("user", "name", "Test User")
        config.set_value("user", "email", "test@example.com")
        config.set_value("commit", "gpgsign", "false")
    (path / "config.yaml").write_text("queue: normal\n")
    repo.index.add(["config.yaml"])
    repo.index.commit("Initial commit")
    return repo


@pytest.fixture
def test_path(tmp_path: Path) -> Path:
    """
    A test_path laid out as the generator does with `use_worktrees`: the base directory on the control
    branch, and a worktree per perturbation branch at <test_path>/<branch>/<repo_dir>.
    """
    test_path = tmp_path / "tests"
    repo = init_repo(test_path / REPO_DIR)
    repo.create_head("ctrl").checkout()
    for branch in ["perturb_1", "perturb_2"]:
        repo.git.worktree("add", "-b", branch, str(test_path / branch / REPO_DIR), "ctrl")
    return test_path


@pytest.fixture
def worktree_indata(test_path: Path) -> dict:
    return {
        "test_path": test_path,
        "repository_directory": REPO_DIR,
        "running_branches": ["ctrl", "perturb_1", "perturb_2"],
        "nruns": [1, 1, 1],
        "keep_uuid": True,
        "startfrom_restart": ["cold", "cold", "cold"],
    }


@pytest.fixture
def payu_sweeps(monkeypatch) -> list:
    """
    Record payu commands instead of running them, and run git commands for real.
    """
    calls = []
    real_run = subprocess.run

    def run(cmd, **kwargs):
        if cmd[0] == "payu":
            calls.append((cmd, Path(kwargs["cwd"])))
            return subprocess.CompletedProcess(cmd, 0)
        return real_run(cmd, **kwargs)

    monkeypatch.setattr(exp_runner.subprocess, "run", run)
    return calls


def listed_worktrees(test_path: Path) -> list[str]:
    listing = git.Repo(test_path / REPO_DIR).git.worktree("list", "--porcelain")
    return [line.removeprefix("worktree ") for line in listing.splitlines() if line.startswith("worktree ")]


def test_run_uses_worktrees_in_place_and_clones_the_rest(test_path, worktree_indata, patch_runner, monkeypatch, capsys):
    def no_repo(path):
        raise AssertionError(f"a worktree must not be fetched or pulled: {path}")

    monkeypatch.setattr(exp_runner.git, "Repo", no_repo)

    exp_runner.ExperimentRunner(worktree_indata).run()

    # only ctrl, which lives in the base directory, is cloned
    assert [call["branch"] for call in patch_runner.payu.clone_calls] == ["ctrl"]
    assert patch_runner.pbs.calls == [
        (test_path / branch / REPO_DIR, 1) for branch in worktree_indata["running_branches"]
    ]
    assert capsys.readouterr().out.count("is a worktree of") == 2


def test_run_rejects_a_worktree_on_another_branch(test_path, worktree_indata, patch_runner):
    # patch_runner replaces git.Repo, so run the git command directly
    git.cmd.Git(test_path / "perturb_1" / REPO_DIR).switch("-c", "other")

    with pytest.raises(RuntimeError, match="instead of branch 'perturb_1'"):
        exp_runner.ExperimentRunner(worktree_indata).run()


def test_base_worktree_head_only_recognises_worktrees_of_the_base_repository(test_path, worktree_indata, tmp_path):
    runner = exp_runner.ExperimentRunner(worktree_indata)
    assert runner._base_worktree_head(test_path / "perturb_1" / REPO_DIR) == "ref: refs/heads/perturb_1"

    # the base directory itself and a plain clone have a .git directory
    assert runner._base_worktree_head(test_path / REPO_DIR) is None
    clone_dir = tmp_path / "clone"
    git.Repo.clone_from(test_path / REPO_DIR, clone_dir)
    assert runner._base_worktree_head(clone_dir) is None

    # a worktree of another repository
    other = init_repo(tmp_path / "other")
    other.git.worktree("add", "-b", "perturb_9", str(tmp_path / "other_worktree"))
    assert runner._base_worktree_head(tmp_path / "other_worktree") is None

    # a .git file that is not a gitdir pointer
    odd = tmp_path / "odd"
    odd.mkdir()
    (odd / ".git").write_text("something else\n")
    assert runner._base_worktree_head(odd) is None


def test_hard_delete_of_a_worktree_prunes_it_and_keeps_the_branch(test_path, worktree_indata, payu_sweeps):
    runner = exp_runner.ExperimentRunner(worktree_indata)

    runner.delete_experiments(branches=["perturb_1"], hard=True)

    assert payu_sweeps == [(["payu", "sweep", "--hard"], test_path / "perturb_1" / REPO_DIR)]
    assert not (test_path / "perturb_1").exists()
    assert str(test_path / "perturb_1" / REPO_DIR) not in listed_worktrees(test_path)
    assert "perturb_1" in git.Repo(test_path / REPO_DIR).heads


def test_remove_repo_dir_refuses_while_other_worktrees_use_it(test_path, worktree_indata, payu_sweeps, capsys):
    runner = exp_runner.ExperimentRunner({**worktree_indata, "running_branches": ["perturb_1"]})

    runner.delete_experiments(all_branches=True, hard=True, remove_repo_dir=True)

    out = capsys.readouterr().out
    assert "still in use by other worktrees" in out
    assert str(test_path / "perturb_2" / REPO_DIR) in out
    assert (test_path / REPO_DIR / ".git").is_dir()


def test_remove_repo_dir_removes_it_once_no_worktree_is_left(test_path, worktree_indata, payu_sweeps):
    runner = exp_runner.ExperimentRunner({**worktree_indata, "running_branches": ["perturb_1", "perturb_2"]})

    runner.delete_experiments(all_branches=True, hard=True, remove_repo_dir=True)

    assert not (test_path / REPO_DIR).exists()
