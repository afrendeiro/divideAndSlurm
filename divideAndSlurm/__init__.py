#!/usr/env python

import re
import os
import time
import string
import cPickle
import textwrap
import subprocess
from collections import OrderedDict
import string, time, random, textwrap

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

        self.name = string.join(["DivideAndSlurm", time.strftime("%Y%m%d%H%M%S", time.localtime())], sep="_")

        self.tmpDir = os.path.abspath(tmpDir)
        self.logDir = os.path.abspath(logDir)

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
        p = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
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
            output, _ = self._slurmSubmitJob(jobFile)
            jobIDs.append(re.sub("\D", "", output))
        task.submissiontime = time.time()
        task.jobIDs = jobIDs

    def cancel_task(self, task):
        """
        Submit slurm jobs with each fraction of data.
        """
        if task not in self.tasks:
            raise AttributeError("Task not in object's tasks.")
        if not hasattr(task, "jobIDs"):
            raise AttributeError("Task does not have jobs initiated.")

        p = subprocess.Popen("squeue | unexpand -t 4 | cut -f 4", stdout=subprocess.PIPE, shell=True)
        processes = p.communicate()[0].split("\n")

        for jobID in task.jobIDs:
            if jobID in processes:
                command = "scancel {0}".format(jobID)
                p = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)            

    def remove_task(self, task):
        """
        Remove task from object.
        """
        #return self.tasks.pop(self.tasks.index(task))
        del self.tasks[self.tasks.index(task)]


class Task(object):
    """
    General class to model tasks to be handled by DivideAndSlurm object.

    It will divide the input data into pools, which can be be submitted in parallel to the cluster by the DivideAndSlurm object.
    Divided input can also be further processed in parallel, taking advantage of all CPUs.
    """
    def __init__(self, data, fractions, *args, **kwargs):
        super(Task, self).__init__()
        
        self.name = time.strftime("%Y%m%d%H%M%S", time.localtime())
        
        # check data is iterable
        if type(data) == dict or type(data) == OrderedDict:
            data = data.items() # implicit type transformation
        self.data = data

        # check fractions is int
        if type(fractions) != int:
            raise TypeError("Fractions must be an integer.")
        self.fractions = fractions
        # additional arguments which can be used by children tasks
        self.args = args

        # permissive output collection
        if "permissive" in kwargs.keys():
            self.permissive = kwargs["permissive"]
        else:
            self.permissive = False

        # Check bunch of stuff
        if "nodes" in kwargs.keys():
            self.nodes = kwargs["nodes"]
        else:
            self.nodes = 1
        if "ntasks" in kwargs.keys():
            self.ntasks = kwargs["ntasks"]
        else:
            self.ntasks = 1
        if "queue" in kwargs.keys():
            self.queue = kwargs["queue"]
        else:
            self.queue = "shortq"
        if "time" in kwargs.keys():
            self.time = kwargs["time"]
        else:
            self.time = "10:00:00"
        if "cpusPerTask" in kwargs.keys():
            self.cpusPerTask = kwargs["cpusPerTask"]
        else:
            self.cpusPerTask = 16
        if "memPerCpu" in kwargs.keys():
            self.memPerCpu = kwargs["memPerCpu"]
        else:
            self.memPerCpu = 2000

    def __repr__(self):
        return self.name

    def __str__(self):
        return self.name

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
        ids = [string.join([self.name, str(i)], sep="_") for i in xrange(len(groups))]
        files = [os.path.join(self.slurm.tmpDir, ID) for ID in ids]
        
        # serialize groups
        for i in xrange(len(groups)):
            pickle.dump(groups[i],                  # actual objects
                open(files[i] + ".input.pickle", 'wb'),  # input pickle file
                protocol=pickle.HIGHEST_PROTOCOL
            )
        return (ids, groups, files)

    def _rm_temps(self):
        """
        If self.output, delete temp files.
        """
        if hasattr(self, "output"):
            for i in xrange(len(self.jobFiles)):
                subprocess.Popen("rm {0}".format(self.jobFiles[i]), stdout=subprocess.PIPE, shell=True)
                subprocess.Popen("rm {0}".format(self.inputPickles[i]), stdout=subprocess.PIPE, shell=True)
                subprocess.Popen("rm {0}".format(self.outputPickles[i]), stdout=subprocess.PIPE, shell=True)

    def _prepare(self):
        """
        Method used to prepare task to be submitted. Should be overwriten by children
        """
        self.log = os.path.join(self.slurm.logDir, string.join([self.name, "log"], sep=".")) # add abspath

        ### Split data in fractions
        ids, groups, files = self._split_data()

        ### Make jobs with groups of data
        self.jobs = list(); self.jobFiles = list(); self.inputPickles = list(); self.outputPickles = list()

        for i in xrange(len(ids)):
            jobFile = files[i] + "_task.sh"
            inputPickle = files[i] + ".input.pickle"
            outputPickle = files[i] + ".output.pickle"

            ###
            # header
            job = self._slurmHeader(ids[i])

            # command - add abspath!
            task = """\

                python script_parallel.py {0} {1} """.format(inputPickle, outputPickle)
            # add more options
            
            job += textwrap.dedent(task)

            # footer
            job += self._slurmFooter()

            # add to save attributes
            self.jobs.append(job)
            self.jobFiles.append(jobFile)
            self.inputPickles.append(inputPickle)
            self.outputPickles.append(outputPickle)

            # write job file to disk
            with open(jobFile, 'w'):
                handle.write(textwrap.dedent(job))

        # Delete data if jobs are ready to submit and data is serialized
        if hasattr(self, "jobs") and hasattr(self, "jobFiles"):
            del self.data

    def is_running(self):
        """
        Returns True if any job from the task is still running.
        """
        # check if all ids are missing from squeue
        p = subprocess.Popen("squeue | unexpand -t 4 | cut -f 4", stdout=subprocess.PIPE, shell=True)
        processes = p.communicate()[0].split("\n")

        if not any([ID in processes for ID in self.jobIDs]):
            return False
        return True

    def has_output(self):
        """
        Returns True is all output pickles are present.
        """
        # check if all output pickles are produced
        if not any([os.path.isfile(outputPickle) for outputPickle in self.outputPickles]):
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

        # if is running or does not have output = not ready
        if self.is_running() or not self.has_output():
            return False
        # if is not running and has output = ready
        self.ready = True
        return True

    def failed(self):
        """
        Check if task failed.
        """
        if self.is_ready() and not self.is_running() and not hasattr(self, "output"):
            return True
        else:
            return False

    def collect(self):
        """
        If self.is_ready(), return joined reduced data.
        """
        if hasattr(self, "output"): # if output is already stored, just return it
            return self.output

        if self.is_ready():
            # load all pickles into list
            if self.permissive:
                outputs = [pickle.load(open(outputPickle, 'r')) for outputPickle in self.outputPickles if os.path.isfile(outputPickle)]
            else:
                outputs = [pickle.load(open(outputPickle, 'r')) for outputPickle in self.outputPickles]
            # if all are counters, and their elements are counters, sum them

            ### THE FOLLOWING PART SHOULD BE SPECIFIC TO EACH TASK:
            if all([type(outputs[i]) == Counter for i in range(len(outputs))]):
                output = reduce(lambda x, y: x + y, outputs) # reduce
                if type(output) == Counter:
                    self.output = output    # store output in object
                    self._rm_temps() # delete tmp files
                    return self.output
        else:
            raise TypeError("Task is not ready yet.")
