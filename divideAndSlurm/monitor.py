#!/usr/env python

"""
"""


import typing as tp
import re
import time
from pathlib import Path

from divideAndSlurm.utils import (
    get_time_string,
    slurm_submit_job,
    slurm_cancel_job,
    get_running_jobs,
)


class Monitor:
    """
    Monitor is a class to handle map-reduce style submission of jobs to a Slurm cluster.

    Example with a Task monitor:
    ```python
    from pathlib import Path
    from divideAndSlurm import Monitor, Task
    m = Monitor(tmp_dir=Path('/tmp'), log_dir=Path('/tmp'), submission_command='sh')

    script = Path("script_parallel.py")
    with open(script, 'w') as fh:
        job = "import sys; from divideAndSlurm.utils import *; n = load_data(sys.argv[1]); save_data([x / 10 for x in n], sys.argv[2])"
        fh.write(job)
    t = Task(data=list(range(10)), fractions=2, script=script)
    m.submit_task(t)
    result = t.collect()
    assert result == [x / 10 for x in list(range(10))]
    ```

    Example with a 'loose' single Task:
    ```python
    from pathlib import Path
    from divideAndSlurm import Task

    script = Path("script_parallel.py")
    with open(script, 'w') as fh:
        job = "import sys; from divideAndSlurm.utils import *; n = load_data(sys.argv[1]); save_data([x / 10 for x in n], sys.argv[2])"
        fh.write(job)
    t = Task(data=list(range(10)), fractions=2, script=script)
    result = t.run(tmp_dir=Path('/tmp'), log_dir=Path('/tmp'), submission_command='sh')
    assert result == [x / 10 for x in list(range(10))]
    ```

    """

    def __init__(
        self,
        tmp_dir: Path = Path("/tmp/"),
        log_dir: Path = Path("/tmp/"),
        submission_command: str = "sbatch",
        user_mail: str = "",
    ):
        super(Monitor, self).__init__()

        self.name = f"Monitor_{get_time_string()}"

        self.tasks: list["Task"] = list()
        self.tmp_dir: Path = Path(tmp_dir).absolute()
        self.log_dir: Path = Path(log_dir).absolute()
        self.submission_command: str = submission_command

        self.user_mail: str = user_mail

    def __repr__(self):
        return f"Monitor object '{self.name}' with {len(self.tasks)} tasks."

    def __str__(self):
        return f"Monitor object '{self.name}' with {len(self.tasks)} tasks."

    def add_task(self, task: "Task") -> None:
        """
        Add Task object to slurm.
        """
        # Object is a Task or of a children class of Task
        assert isinstance(task, Task), "Object provided is not a Task object."

        self.tasks.append(task)
        task.monitor = self
        task._prepare()

    def remove_task(self, task: "Task") -> None:
        """
        Remove task from object.
        """
        # return self.tasks.pop(self.tasks.index(task))
        del self.tasks[self.tasks.index(task)]

    def submit_task(self, task: "Task") -> None:
        """
        Submit slurm jobs with each fraction of data.
        """
        if task not in self.tasks:
            self.add_task(task)
        if not hasattr(task, "jobs") or not hasattr(task, "job_files"):
            raise AttributeError("Task does not have jobs to be submitted.")

        job_ids = list()
        for job_file in task.job_files:
            job_id = slurm_submit_job(job_file, self.submission_command)
            job_ids.append(job_id)
        task.submission_time = time.time()
        task.job_ids = job_ids

    def cancel_task(self, task: "Task") -> None:
        """
        Cancel running task job.
        """
        if task not in self.tasks:
            raise AttributeError("Task not in object's tasks.")
        if not hasattr(task, "job_ids"):
            raise AttributeError("Task does not have jobs initiated.")

        running_jobs = get_running_jobs()
        for job_id in task.job_ids:
            if job_id in running_jobs:
                slurm_cancel_job(job_id)

    def cancel_all_tasks(self, task: "Task") -> None:
        """
        Cancel running task jobs.
        """
        raise NotImplementedError("")


from divideAndSlurm.task import Task
