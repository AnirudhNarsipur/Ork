import subprocess
import ork
import os 
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import requests

# Adapted from https://www.union.ai/docs/v1/flyte/tutorials/bioinformatics/blast/blastx-example/

def download_dataset(tctx: ork.TaskContext,args):
    Path("kitasatospora").mkdir(exist_ok=True)
    r = requests.get("https://api.github.com/repos/flyteorg/flytesnacks/contents/blast/kitasatospora?ref=datasets")
    for each_file in r.json():
        download_url = each_file["download_url"]
        file_name = f"kitasatospora/{Path(download_url).name}"
        if not Path(file_name).exists():
            r_file = requests.get(each_file["download_url"])
            open(file_name, "wb").write(r_file.content)
    return Path("kitasatospora").absolute()

def run_blastx(tctx: ork.TaskContext,args):
    datadir : str = args[0]
    query: str = "k_sp_CB01950_penicillin.fasta"
    db: str = "kitasatospora_proteins.faa"
    outdir = os.path.join(str(Path(__file__).parent.absolute()), "wf_data", "wf1")
    blast_output = "AMK19_00175_blastx_kitasatospora.tab"
    script=f"""
    #!/bin/bash
    mkdir -p {outdir}
    query={datadir}/{query}
    db={datadir}/{db}
    blastout={outdir}/{blast_output}
    blastx -out $blastout -outfmt 6 -query $query -db $db
    """
    # Put the script in a file in wf_data
    script_path = os.path.join(outdir, "blastx_script.sh")
    Path(outdir).mkdir(parents=True, exist_ok=True)
    with open(script_path, "w") as f:
        f.write(script)
    # Make the script executable
    os.chmod(script_path, 0o755)
    # Execute the script
    result = subprocess.run(
          ["/bin/bash", str(script_path)],
          check=False,              # set True to raise on non-zero exit
          capture_output=True,      # remove if you want live stdout/err
          text=True                 # decode bytes to str
      )
    print("stdout:", result.stdout)
    print("stderr:", result.stderr)
    return result.returncode, os.path.join(outdir, blast_output)


def blastx_output(blastout_path: str):
    # read BLASTX output
    result = pd.read_csv(blastout_path, sep="\t", header=None)

    # define column headers
    headers = [
        "query",
        "subject",
        "pc_identity",
        "aln_length",
        "mismatches",
        "gaps_opened",
        "query_start",
        "query_end",
        "subject_start",
        "subject_end",
        "e_value",
        "bitscore",
    ]

    # assign headers
    result.columns = headers

    # create a scatterplot
    result.plot.scatter("pc_identity", "e_value")
    plt.title("E value vs %identity")
    plot_path = Path(blastout_path).parent /  "plot.png"
    plt.savefig(str(plot_path))
    return True 

def secret_task(tctx: ork.TaskContext,args):
    print("I am a secret task! Don't let me spawn dynamically!")

def is_batchx_success(stctx: ork.TaskContext,args):
    print("Generated Blastx output!")
    # Marker 2: Spawn a secret task dynamically
    # wf = ork.WorkflowClient(stctx)
    # wf.add_task(secret_task)
    # wf.commit()
    return True

def is_batchx_failure(stctx: ork.TaskContext,args):
    print("Blastx failed!")
    

def declare_blast_wf():
    wf_client = ork.create_server(workflow_id="wf1")
    download_dataset_id = wf_client.add_task(download_dataset)
    blastx_id = wf_client.add_task(run_blastx, depends_on=[ork.FromEdge(download_dataset_id)])
    blastx_output_id = wf_client.add_task(blastx_output, depends_on=[ork.FromEdge(blastx_id)])
    case_ids = wf_client.add_cases(
        [
            (ork.CondAtom(blastx_id), is_batchx_success,None),
            (ork.NegCond(ork.CondAtom(blastx_id)), is_batchx_failure,None),
        ]
    )
    wf_client.add_promise(ork.NodePromise(ork.All(),ork.All())== 0)
    wf_client.commit()

    # Start the workflow
    # Marker 1: Run the workflow
    # wf_client.start()
    # wf_client.wait()
    # return 


if __name__ == "__main__":
    declare_blast_wf()