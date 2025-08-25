import pytest
from pathlib import Path
import experiment_runner.experiment_runner as exp_runner


class DummyBranch:
    def __init__(self, name: str):
        self.name = name


class DummyCommit:
    def __init__(self, hexsha: str):
        self.hexsha = hexsha


class DummyHead:
    def __init__(self, name: str, commit: DummyCommit):
        self.name = name
        self.commit = commit


class DummyHeadContainer:
    def __init__(self, commit):
        self.commit = commit


class DummyGit:
    """
    This mimics repo.git commands - checkout, pull, reset, diff etc.
    """

    def __init__(self, repo):
        self._repo = repo

    def checkout(self, *args):
        # checkout <branch> or checkout -b <branch> origin/<branch>
        if len(args) == 1:
            target = args[0]
            self._repo._checkout_existing_branch(target)
        elif len(args) == 3 and args[0] == "-b" and args[2].startswith("origin/"):
            target = args[1]
            self._repo._create_and_checkout_branch(target)

    def pull(self, *args):
        if self._repo._pull_raises:
            raise self._repo._exc.GitCommandError("Simulated pull error")
        if self._repo._new_commit_hash is not None:
            self._repo.head.commit = DummyCommit(self._repo._new_commit_hash)

    def reset(self, *args):
        if self._repo._new_commit_hash is not None:
            self._repo.head.commit = DummyCommit(self._repo._new_commit_hash)

    def diff(self, *args):
        return self._repo._diff_output


class DummyRemote:
    def __init__(self, repo):
        self._repo = repo

    def fetch(self, prune=False):
        self._repo._fetch_called = True


class DummyRemotes:
    def __init__(self, repo):
        self.origin = DummyRemote(repo)


class DummyRepo:
    def __init__(self, path: Path):
        self.path = path
        self.heads = {}  # name -> DummyHead
        self.head = DummyHeadContainer(commit=DummyCommit("initial"))
        self.remotes = DummyRemotes(self)
        self.git = DummyGit(self)
        self._fetch_called = False
        self._pull_raises = False
        self._new_commit_hash = None
        self._diff_output = ""
        self._fetch_called = False

    def _checkout_existing_branch(self, branch_name: str):
        if branch_name not in self.heads:
            raise self._GitCommandError(f"Branch {branch_name} is missing!")
        # self.head.commit = self.heads[branch_name].commit

    def _create_and_checkout_branch(self, branch_name: str):
        self.heads[branch_name] = DummyHead(branch_name, self.head.commit)


class PayuCalls:
    def __init__(self):
        self.clone_calls = []
        self.list_calls = []


def _dummy_clone(repository, directory: str, branch, **kwargs):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "config.yaml").write_text("queue: normal\n")

    _PAYU_CALLS.clone_calls.append(
        {
            "repository": str(repository),
            "directory": str(directory),
            "branch": branch,
            "kwargs": kwargs,
        }
    )


def _dummy_list_branches(config_path: Path) -> None:
    _PAYU_CALLS.list_calls.append(Path(config_path))


_PAYU_CALLS = PayuCalls()


@pytest.fixture
def payu_calls():
    global _PAYU_CALLS
    _PAYU_CALLS = PayuCalls()
    return _PAYU_CALLS


class DummyPbsJobManager:
    def __init__(self):
        self.calls = []

    def pbs_job_runs(self, expt: Path, nrun: int):
        self.calls.append((expt, nrun))


@pytest.fixture
def pbs_job_recorder():
    return DummyPbsJobManager()


@pytest.fixture
def patch_runner(monkeypatch, tmp_path, payu_calls, pbs_job_recorder):
    """
    Patch external calls in experiment_runner.experiment_runner.
    - payu.branch.clone, payu.branch.list_branches
    - PBSJobManager
    - git.Repo
    """
    monkeypatch.setattr(exp_runner, "clone", _dummy_clone, raising=True)
    monkeypatch.setattr(exp_runner, "list_branches", _dummy_list_branches, raising=True)
    monkeypatch.setattr(exp_runner, "PBSJobManager", lambda: pbs_job_recorder, raising=True)

    class _Exc:
        class GitCommandError(Exception):
            pass

    monkeypatch.setattr(exp_runner.git, "exc", _Exc, raising=True)

    def _make_repo(path):
        repo = DummyRepo(path)
        repo._exc = exp_runner.git.exc  # store the exc container here
        return repo

    monkeypatch.setattr(exp_runner.git, "Repo", _make_repo, raising=True)

    class Controls:
        pass

    controls = Controls()
    controls.make_repo = _make_repo
    controls.payu = payu_calls
    controls.pbs = pbs_job_recorder
    return controls


@pytest.fixture
def indata(tmp_path: Path) -> dict:
    return {
        "test_path": tmp_path / "tests",
        "repository_directory": "test_repo",
        "running_branches": ["perturb_1", "perturb_2"],
        "nruns": [1, 2],
        "keep_uuid": True,
    }
