#!/usr/bin/env python3
"""This script generates the job submission script on OSG"""


import sys
from os import path
import random

FILENAME = "singularity.xml"

def print_usage():
    """This function prints out help messages"""
    print("Usage: {} ".format(sys.argv[0].split("/")[-1])
          + "Njobs Nevents_per_job N_threads SingularityImage Outputpath ParameterFile "
          + "jobId [bayesFile]")


def write_submission_script(para_dict_):
    jobName = "iEBEMUSIC_{}".format(para_dict_["job_id"])
    random_seed = random.SystemRandom().randint(0, 10000000)
    imagePathHeader = "osdf://"
    script = open(FILENAME, "w")
    script.write("""<?xml version="1.0" encoding="UTF-8"?>
<job>
    <command>""")
                 
    if para_dict_["bayesFlag"]:
        script.write("""./run_singularity.sh {0} $(Process) {1} {2} {3} {4} </command>
""".format(para_dict_["paraFile"], para_dict_["n_events_per_job"],
           para_dict_["n_threads"], random_seed, para_dict_["bayesFile"]))
    else:
        script.write("""./run_singularity.sh {0} $(Process) {1} {2} {3} </command>
""".format(para_dict_["paraFile"], para_dict_["n_events_per_job"],
           para_dict_["n_threads"], random_seed))
    script.write("""
    <shell>/bin/sh -c 'exec singularity exec -e -B /direct -B /star -B /afs -B /gpfs {0}</shell>""".format(para_dict_["image_with_path"]))
    
    script.write("""    
    <ResourceUsage>
        <Memory>
            <MinMemory>2024</MinMemory>
        </Memory>
        <StorageSpace>
            <MinStorage>2024</MinStorage>
        </StorageSpace>
    </ResourceUsage>""")
    script.write("""
    <SandBox>
        <Package>
            <File>file: ./run_singularity.sh </File>
            <File>file: {0} </File>
            <File>file: /star/u/maxwoo/iEBE-MUSIC/ </File>""".format(para_dict_["paraFile"]))

    if para_dict_['bayesFlag']:
        script.write("""
            <File>file: {0} </File>
""".format(para_dict_['bayesFile']))
        
    script.write("""
        </Package>
    </SandBox>
    <stderr URL="file:{0}/log/job.$(Cluster).$(Process).error" />
    <stdout URL="file:{0}/log/job.$(Cluster).$(Process).output" />
    <output fromScratch="./playground/event_0/EVENT_RESULT_*/*.h5" toURL="{0}/data/"/>
    <output fromScratch="./playground/event_0/EVENT_RESULT_*/*.gz" toURL="{0}/data/"/>
</job>""".format(para_dict_['output_path']))

    script.close()


def write_job_running_script(para_dict_):
    script = open("run_singularity.sh", "w")
    script.write("""#!/usr/bin/env bash

source ~/venv/bin/activate.csh
parafile=$1
processId=$2
nHydroEvents=$3
nthreads=$4
seed=$5

# Run the singularity container
export PYTHONIOENCODING=utf-8
export PATH="${PATH}:/usr/lib64/openmpi/bin:/usr/local/gsl/2.5/x86_64/bin"
export LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:/usr/local/lib:/usr/local/gsl/2.5/x86_64/lib64"

printf "Start time: `/bin/date`\\n"
printf "Job is running on node: `/bin/hostname`\\n"
printf "system kernel: `uname -r`\\n"
printf "Job running as user: `/usr/bin/id`\\n"

""")
    if para_dict_["bayesFlag"]:
        script.write("""bayesFile=$6

/home/iEBE-MUSIC/generate_jobs.py -w playground -c OSG -par ${parafile} -id ${processId} -n_th ${nthreads} -n_urqmd ${nthreads} -n_hydro ${nHydroEvents} -seed ${seed} -b ${bayesFile} --nocopy --continueFlag
""")
    else:
        script.write("""
/home/iEBE-MUSIC/generate_jobs.py -w playground -c OSG -par ${parafile} -id ${processId} -n_th ${nthreads} -n_urqmd ${nthreads} -n_hydro ${nHydroEvents} -seed ${seed} --nocopy --continueFlag
""")

    script.write("""
cd playground/event_0
mv EVENT_RESULTS_${processId}.tar.gz playground/event_0
bash submit_job.pbs
status=$?
if [ $status -ne 0 ]; then
    exit $status
fi
""")
    script.close()


def main(para_dict_):
    write_submission_script(para_dict_)
    write_job_running_script(para_dict_)


if __name__ == "__main__":
    bayesFlag = False
    bayesFile = ""
    try:
        N_JOBS = int(sys.argv[1])
        N_EVENTS_PER_JOBS = int(sys.argv[2])
        N_THREADS = int(sys.argv[3])
        SINGULARITY_IMAGE_PATH = sys.argv[4]
        SINGULARITY_IMAGE = SINGULARITY_IMAGE_PATH.split("/")[-1]
        OUTPUT_PATH = sys.argv[5]
        PARAMFILE = sys.argv[6]
        JOBID = sys.argv[7]
        if len(sys.argv) == 9:
            bayesFile = sys.argv[8]
            bayesFlag = True
    except (IndexError, ValueError) as e:
        print_usage()
        exit(0)

    para_dict = {
        'n_jobs': N_JOBS,
        'n_events_per_job': N_EVENTS_PER_JOBS,
        'n_threads': N_THREADS,
        'image_name': SINGULARITY_IMAGE,
        'image_with_path': SINGULARITY_IMAGE_PATH,
        'output_path': OUTPUT_PATH,
        'paraFile': PARAMFILE,
        'job_id': JOBID,
        'bayesFlag': bayesFlag,
        'bayesFile': bayesFile,
    }

    main(para_dict)

