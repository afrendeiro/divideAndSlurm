#!/usr/env python

"""
"""

import typing as tp
import time
import subprocess
import pickle
from pathlib import Path


def get_time_string() -> str:
    return time.strftime("%Y-%m-%d:%H-%M-%S")


def get_running_jobs() -> list[str]:
    p = subprocess.Popen(
        "squeue | unexpand -t 4 | cut -f 4", stdout=subprocess.PIPE, shell=True
    )
    processes = p.communicate()[0].decode().split("\n")
    return processes


def remove_file(file: Path) -> None:
    subprocess.Popen(
        f"rm {file}",
        stdout=subprocess.PIPE,
        shell=True,
    )


def slurm_submit_job(job_file: Path, executable: str = "sbatch") -> tp.Optional[int]:
    """
    Submit command to shell.
    """
    command = f"{executable} {job_file}"
    process = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
    output, _ = process.communicate()
    output = output.decode()
    if executable == "sbatch":
        return int(re.sub(r"\D", "", output))
    return process.pid


def slurm_cancel_job(job_id: int) -> None:
    command = f"scancel {job_id}"
    p = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)


def save_data(data: tp.Any, pickle_file: Path) -> None:
    return pickle.dump(data, open(pickle_file, "wb"))


def load_data(pickle_file: Path) -> tp.Any:
    return pickle.load(open(pickle_file, "rb"))
