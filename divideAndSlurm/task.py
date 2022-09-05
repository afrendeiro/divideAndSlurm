#!/usr/env python

"""
"""

import typing as tp
import random
import pickle
import textwrap
import subprocess
from collections import OrderedDict, Counter
from pathlib import Path
from functools import reduce

from divideAndSlurm.utils import remove_file, get_running_jobs, get_time_string


class Task:
    """
    General class to model tasks to be handled by Monitor object.

    It will divide the input data into pools, which can be be submitted in parallel to the cluster by the Monitor object.
    Divided input can also be further processed in parallel, taking advantage of all CPUs.

    kwargs passed uppon initialization are:
        - permissive
        - nodes
        - ntasks
        - queue
        - time
        - cpus_per_task
        - mem_per_cpu
    """

    def __init__(
        self,
        data: tp.Iterable,
        fractions: int,
        script: Path,
        executable: str = "python",
        reduce_func: tp.Callable = None,
        *args,
        **kwargs,
    ):
        super(Task, self).__init__()

        now = f"{get_time_string()}_{random.randint(1, 1000)}"
        self.name = f"task_{now}"

        # check data is iterable
        if isinstance(data, (dict, OrderedDict)):
            data = data.items()  # implicit type transformation
        self.data = data

        # check fractions is int
        if not isinstance(fractions, int):
            raise TypeError("Fractions must be an integer.")
        self.fractions = fractions
        self.script = script.absolute()
        self.executable = executable
        # additional arguments which can be used by child tasks
        self.args = args

        # permissive output collection
        if "permissive" in kwargs.keys():
            self.permissive = kwargs["permissive"]
        else:
            self.permissive = False

        defaults = dict(
            nodes=1,
            ntasks=1,
            queue="shortq",
            time="10:00:00",
            cpus_per_task=2,
            mem_per_cpu=2000,
        )
        defaults.update(kwargs)
        for k, v in defaults.items():
            setattr(self, k, v)

        self.log_file: Path
        self.job_ids: list[str] = list()
        self.jobs: list[str] = list()
        self.job_files: list[Path] = list()
        self.input_pickles: list[Path] = list()
        self.output_pickles: list[Path] = list()

        self.queue: str
        self.ntasks: int
        self.time: str
        self.cpus_per_task: int
        self.mem_per_cpu: int
        self.nodes: int

        self.monitor: "Monitor"

        self.reduce_func = reduce_func

    def __repr__(self):
        return self.name

    def __str__(self):
        return self.name

    def get_slurm_header(self, job_id: str):
        command = f"""            #!/bin/bash
            #SBATCH --partition={self.queue}
            #SBATCH --ntasks={self.ntasks}
            #SBATCH --time={self.time}

            #SBATCH --cpus-per-task={self.cpus_per_task}
            #SBATCH --mem-per-cpu={self.mem_per_cpu}
            #SBATCH --nodes={self.nodes}

            #SBATCH --job-name={job_id}
            #SBATCH --output={self.log_file}

            #SBATCH --mail-type=end
            #SBATCH --mail-user={self.monitor.user_mail}

            # Start running the job
            hostname
            date

            """
        return textwrap.dedent(command)

    def get_slurm_footer(self) -> str:
        command = """

            # Job end
            date
        """
        return textwrap.dedent(command)

    def _split_data(self) -> None:
        """
        Split data in fractions and create pickle objects with them.
        """

        def _chunkify(lst, n):
            """Yield successive n-sized chunks from lst."""
            for i in range(0, len(lst), n):
                yield lst[i : i + n]

        groups = _chunkify(self.data, self.fractions)
        for i, group in enumerate(groups):
            id_ = f"{self.name}_{i}"
            job_file = self.monitor.tmp_dir / (id_ + ".task.sh")
            input_pickle = self.monitor.tmp_dir / (id_ + ".input.pickle")
            output_pickle = self.monitor.tmp_dir / (id_ + ".output.pickle")
            with open(input_pickle, "wb") as fh:
                pickle.dump(group, fh)
            self.job_ids.append(id_)
            self.job_files.append(job_file)
            self.input_pickles.append(input_pickle)
            self.output_pickles.append(output_pickle)

    def _prepare(self) -> None:
        """
        Method used to prepare task to be submitted. Can be overwriten by children
        """
        self.log_file = self.monitor.log_dir / f"{self.name}.log"

        # Split data in fractions
        self._split_data()

        for job_id, job_file, input_pickle, output_pickle in zip(
            self.job_ids, self.job_files, self.input_pickles, self.output_pickles
        ):
            task = f"""{self.executable} {self.script} {input_pickle} {output_pickle}"""
            job = (
                self.get_slurm_header(job_id)
                + textwrap.dedent(task)
                + self.get_slurm_footer()
                + "\n"
            )
            with open(job_file, "w") as fh:
                fh.write(job)
            self.jobs.append(job)

        # Delete data if jobs are ready to submit and data is serialized
        if self.jobs and all([j.exists() for j in self.job_files]):
            del self.data

    def is_running(self) -> bool:
        """
        Returns True if any job from the task is still running.
        """
        # check if all ids are missing from squeue
        running_jobs = get_running_jobs()
        if not any([id_ in running_jobs for id_ in self.job_ids]):
            return False
        return True

    def has_output(self) -> bool:
        """
        Returns True is all output pickles are present.
        """
        # check if all output pickles are produced
        if not any([output_pickle.exists() for output_pickle in self.output_pickles]):
            return False
        return True

    def is_ready(self) -> bool:
        """
        Check if all submitted jobs have been completed.
        """
        if hasattr(self, "ready"):  # if already finished
            return True
        if not self.job_ids:  # if not even started
            return False

        # if is running or does not have output = not ready
        if self.is_running() or not self.has_output():
            return False
        # if is not running and has output = ready
        self.ready = True
        return True

    def failed(self) -> bool:
        """
        Check if task failed.
        """
        if self.is_ready() and not self.is_running() and not hasattr(self, "output"):
            return True
        else:
            return False

    def _rm_temps(self):
        """
        If self.output, delete temp files.
        """
        if hasattr(self, "output"):
            for i in range(len(self.job_files)):
                for f in [
                    self.job_files[i],
                    self.input_pickles[i],
                    self.output_pickles[i],
                ]:
                    remove_file(f)

    def collect(self):
        """
        If self.is_ready(), return joined reduced data.
        """
        if hasattr(self, "output"):  # if output is already stored, just return it
            return self.output
        if not self.is_ready():
            raise TypeError("Task is not ready yet.")

        # load all pickles into list
        if self.permissive:
            outputs = (
                pickle.load(open(output_pickle, "rb"))
                for output_pickle in self.output_pickles
                if output_pickle.exists()
            )
        else:
            outputs = (
                pickle.load(open(output_pickle, "rb"))
                for output_pickle in self.output_pickles
            )

        if self.reduce_func is not None:
            return self.reduce_func(outputs)

        # THE FOLLOWING PART SHOULD BE SPECIFIC TO EACH TASK:
        outputs = list(outputs)  # TODO: re-write to make use of generator

        # List of lists:
        if all([isinstance(output, list) for output in outputs]):
            self.output = [y for x in outputs for y in x]
            self._rm_temps()
            return self.output

        # Counters: and their elements are counters, sum them
        if all([isinstance(output, Counter) for output in outputs]):
            output = reduce(lambda x, y: x + y, outputs)
            if isinstance(output, Counter):
                self.output = output  # store output in object
                self._rm_temps()  # delete tmp files
                return self.output

    def submit(self, **kwargs):
        if not hasattr(self, "monitor"):
            self.monitor = Monitor(**kwargs)
        self.monitor.submit_task(self)

    def run(self, **kwargs):
        self.submit(**kwargs)
        return self.collect()


from divideAndSlurm.monitor import Monitor
