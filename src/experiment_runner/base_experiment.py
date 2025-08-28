from pathlib import Path


class BaseExperiment:
    """
    Initialise with configuration data.
    """

    def __init__(self, indata: dict):
        self.indata = indata
        self.test_path = indata.get("test_path", None)
        self.model_type = indata.get("model_type", False)
        self.repository_directory = indata.get("repository_directory")
        self.base_directory = Path(self.test_path) / self.repository_directory
        self.running_branches = indata.get("running_branches", None)
        self.nruns = indata.get("nruns", 0)
        self.keep_uuid = indata.get("keep_uuid", False)
        self.config_path = indata.get("config_path", None)
        self.lab_path = indata.get("lab_path", None)
        self.new_branch_name = indata.get("new_branch_name", None)
        self.restart_path = indata.get("restart_path", None)
        self.start_point = indata.get("start_point", None)
        self.parent_experiment = indata.get("parent_experiment", None)
        self.startfrom_restart: list[str] = indata.get("startfrom_restart", ["cold"])
