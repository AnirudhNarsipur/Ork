import subprocess
import ork
import os 
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import requests

# Adapted from https://www.union.ai/docs/v1/flyte/tutorials/bioinformatics/blast/blastx-example/
WF1_DIR = os.path.join(str(Path(__file__).parent.parent.absolute()), "wf_data", "wf1")
def download_dataset(tctx: ork.TaskContext,args):
    print("Beginning dataset download...")
    input_dir_path = (Path(WF1_DIR) / "kitasatospora")
    input_dir_path.mkdir(exist_ok=True)
    r = requests.get("https://api.github.com/repos/flyteorg/flytesnacks/contents/blast/kitasatospora?ref=datasets")
    for each_file in r.json():
        download_url = each_file["download_url"]
        file_path = input_dir_path / Path(download_url).name
        if not file_path.exists():
            r_file = requests.get(each_file["download_url"])
            open(str(file_path), "wb").write(r_file.content)
    print("Dataset download complete.")
    return input_dir_path

def run_blastx(tctx: ork.TaskContext,args):
    print("Starting BLASTX...")
    datadir : str = args[0]
    query: str = "k_sp_CB01950_penicillin.fasta"
    db: str = "kitasatospora_proteins.faa"
    outdir = os.path.join(str(Path(__file__).parent.parent.absolute()), "wf_data", "wf1")
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
    print("blastx stdout:", result.stdout)
    print("blastx stderr:", result.stderr)
    return result.returncode, os.path.join(outdir, blast_output)


def blastx_output(tctx: ork.TaskContext,args):
    print("Processing BLASTX output...")
    # read BLASTX output
    blastout_path = args[0][1]
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
    print(f"Saved BLASTX plot to {plot_path}")
    # Marker 4: Return false to take a different branch
    # return False
    return True 

def secret_task(tctx: ork.TaskContext,args):
    print("I am a secret task! Don't let me spawn dynamically!")

def is_batchx_success(stctx: ork.TaskContext,args):
    print("Generated Blastx output successfully!")
    # Marker 2: Spawn a secret task dynamically
    # wf = ork.WorkflowClient(stctx)
    # wf.add_task(secret_task)
    # wf.commit()
    # print("Spawned secret task dynamically!")
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
            (ork.CondAtom(blastx_output_id), is_batchx_success,None),
            (ork.NegCond(ork.CondAtom(blastx_output_id)), is_batchx_failure,None),
        ]
    )
    # Marker 3: Add a promise that no further tasks can be created after this point
    # wf_client.add_promise(ork.NodePromise(ork.All(),ork.All())== 0)
    wf_client.commit()
    print(f"Have workflow ids: download_dataset_id={download_dataset_id}, blastx_id={blastx_id}, blastx_output_id={blastx_output_id}, case_ids={case_ids}")

    # Start the workflow
    # Marker 1: Run the workflow
    # wf_client.start()
    # wf_client.wait()


if __name__ == "__main__":
    declare_blast_wf()