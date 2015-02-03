#!/usr/env python

import os as _os
import time as _time
import string as _string
import cPickle as _pickle
import textwrap as _textwrap
import subprocess as _subprocess
from collections import OrderedDict

__author__ = "Andre Rendeiro"
__copyright__ = "Copyright 2014, Andre F. Rendeiro"
__credits__ = []
__license__ = "GPL3"
__version__ = "0.1"
__maintainer__ = "Andre Rendeiro"
__email__ = "arendeiro@cemm.oeaw.ac.at"
__status__ = "Development"


class DivideAndSlurm(object):
    """
    DivideAndSlurm is a class to handle a map-reduce style submission of jobs to a Slurm cluster.
    """
    def __init__(self, tmpDir="/fhgfs/scratch/users/arendeiro/", logDir="/home/arendeiro/logs", userMail=""):
        super(DivideAndSlurm, self).__init__()

        self.tasks = list()

        self.name = _string.join(["DivideAndSlurm", _time.strftime("%Y%m%d%H%M%S", _time.localtime())], sep="_")

        self.tmpDir = _os.path.abspath(tmpDir)
        self.logDir = _os.path.abspath(logDir)

        self.userMail = userMail

    def __repr__(self):
        return "DivideAndSlurm object " + self.name

    def __str__(self):
        return "DivideAndSlurm object " + self.name

    def _slurmSubmitJob(self, jobFile):
        """
        Submit command to shell.
        """
        command = "sbatch %s" % jobFile
        p = _subprocess.Popen(command, stdout=_subprocess.PIPE, shell=True)
        return p.communicate()

    def add_task(self, task):
        """
        Add Task object to slurm.
        """
        # Object is a Task or of a children class of Task
        if isinstance(task, Task):
            self.tasks.append(task)
            task.slurm = self
            task._prepare()
        else:
            raise TypeError("Object provided is not a Task object.")

    def submit(self, task):
        """
        Submit slurm jobs with each fraction of data.
        """
        if task not in self.tasks:
            raise AttributeError("Task not in object's tasks.")
        if not hasattr(task, "jobs") or not hasattr(task, "jobFiles"):
            raise AttributeError("Task does not have jobs to be submitted.")

        jobIDs = list()
        for jobFile in task.jobFiles:
            output, err = self._slurmSubmitJob(jobFile)
            jobIDs.append(re.sub("\D", "", output))
        task.submission_time = _time.time()
        tasks.jobIDs = jobIDs

    def cancel_task(self, task):
        """
        Submit slurm jobs with each fraction of data.
        """
        if task not in self.tasks:
            raise AttributeError("Task not in object's tasks.")
        if not hasattr(task, "jobIDs"):
            raise AttributeError("Task does not have jobs initiated.")
        for jobID in task.jobIDs:
            command = "scancel %s" % jobID
            p = _subprocess.Popen(command, stdout=_subprocess.PIPE, shell=True)            

    def remove_task(self, task):
        """
        Remove task from object.
        """
        return self.tasks.pop(self.tasks.index(task))



class Task(object):
    """
    General class to model tasks to be handled by DivideAndSlurm object.

    It will divide the input data into pools, which can be be submitted in parallel to the cluster by the DivideAndSlurm object.
    Divided input can also be further processed in parallel, taking advantage of all CPUs.
    """
    def __init__(self, data, fractions, queue="shortq", ntasks=1, time="10:00:00", cpusPerTask=16, memPerCpu=2000, nodes=1):
        #super(Task, self).__init__()

        self.name = "Task created at {0}".format(_time.strftime("%Y%m%d%H%M%S", _time.localtime()))

        # check data is iterable
        if type(data) == dict or type(data) == OrderedDict:
            data = data.items() # implicit type transformation
        self.data = data
        # check fractions is int
        if type(fractions) != int:
            raise TypeError("Fractions must be an integer.")
        self.fractions = fractions

    def __repr__(self):
        return "Task object " + self.name

    def __str__(self):
        return "Task object " + self.name

    def _slurmHeader(self, jobID):
        command = """            #!/bin/bash
            #SBATCH --partition={0}
            #SBATCH --ntasks={1}
            #SBATCH --time={2}

            #SBATCH --cpus-per-task={3}
            #SBATCH --mem-per-cpu={4}
            #SBATCH --nodes={5}

            #SBATCH --job-name={6}
            #SBATCH --output={7}

            #SBATCH --mail-type=end
            #SBATCH --mail-user={8}

            # Start running the job
            hostname
            date

            """.format(self.queue, self.ntasks, self.time, self.cpusPerTask,
                self.memPerCpu, self.nodes, jobID, self.log, self.slurm.userMail
            )
        return textwrap.dedent(command)

    def _slurmFooter(self):
        command = """

            # Job end
            date
        """
        return textwrap.dedent(command)

    def _split_data(self):
        """
        Split data in fractions and create pickle objects with them.
        """
        chunkify = lambda lst,n: [lst[i::n] for i in xrange(n)]

        groups = chunkify(self.data, self.fractions)
        ids = [_string.join([self.name, str(i)], sep="_") for i in xrange(len(groups))]
        files = [_os.path.join(self.slurm.tmpDir, ID) for ID in ids]
        
        # serialize groups
        for i in xrange(len(groups)):
            _pickle.dump(groups[i],                  # actual objects
                open(files[i] + ".input.pickle", 'wb'),  # input pickle file
                protocol=_pickle.HIGHEST_PROTOCOL
            )
        return (ids, groups, files)

    def _rm_temps(self):
        """
        If self.output, delete temp files.
        """
        if hasattr(self, "output"):
            for i in xrange(len(self.jobFiles)):
                p = _subprocess.Popen("rm {0}".format(self.jobFiles[i]), stdout=_subprocess.PIPE, shell=True)
                p = _subprocess.Popen("rm {0}".format(self.inputPickles[i]), stdout=_subprocess.PIPE, shell=True)
                p = _subprocess.Popen("rm {0}".format(self.outputPickles[i]), stdout=_subprocess.PIPE, shell=True)

    def _prepare(self):
        """
        Method used to prepare task to be submitted. Should be overwriten by children as it is specific to a particular task.
        """
        self.log = os.path.join(self.slurm.logDir, string.join([self.name, "log"], sep=".")) # add abspath
        # self._prepare()

    def is_running(self):
        """
        Returns True if any job from the task is still running.
        """
        # check if all ids are missing from squeue
        p = _subprocess.Popen("squeue | unexpand -t 4 | cut -f 4", stdout=_subprocess.PIPE, shell=True)
        processes = p.communicate()[0].split("\n")

        if not any([ID in processes for ID in self.jobIDs]):
            return False
        return True

    def has_output(self):
        """
        Returns True is all output pickles are present.
        """
        # check if all output pickles are produced
        if not any([_os.path.isfile(outputPickle) for outputPickle in self.outputPickles]):
            return False
        return True

    def is_ready(self):
        """
        Check if all submitted jobs have been completed.
        """
        if hasattr(self, "ready"): # if already finished
            return True
        if not hasattr(self, "jobIDs"): # if not even started
            return False

        # if is not running and has output
        if self.is_running() or not self.has_output():
            return False
        self.ready = True
        return True

    def collect(self):
        return None
