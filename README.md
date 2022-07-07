# Qualtrics Ingestion
This ETL Extracts Transforms and Loads.

## Get Started

### Development with vscode and `devcontainer.json`

We can use `Containerfile` to develop in, but first we must install the **prerequisites**.

* [Install vscode](https://code.visualstudio.com/download)
* [Install vscode: Remote Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)
* [Install docker](https://docs.docker.com/get-docker/) (podman is not supported without some hacking)

Once you are done just open this project directory in vscode. You should see a prompt in the bottom right to reopen in container.

All remote container functions can be accessed from the green item in the left of the bottom bar.

### Setup Deploy Process

**Note:** If using the devcontainer `just` and `oc` are already installed.
* Run `just oc-deploy` to push configs to Openshift.
* Add the secrets (usernames, passwords, credential files) your ETL needs to your openshift namespace [here](https://console-openshift-console.apps.ssa-prod.20r1.p1.openshiftapps.com/k8s/ns/airflow-prod-ssa-eda/secrets). Your secrets should be separated by service and account. For instance your SSA Redshift username and password should be in one secret. 
* Edit and Add the file `dag_qualtrics_ingestion.py` to the airflow git repo [here](https://gitlab.corp.redhat.com/it-ssa/ssa-eda/airflow-dags)
* Clone the [airflow-dags-meta repo](https://gitlab.corp.redhat.com/it-ssa/airflow-dags-meta/)
* From that repo run `./update_submodule.sh ssa-eda`
Follow the links from the output of that script to Approve and Merge the submodule update.

### Deploy

* Once setup using the above instructions this repo will auto deploy to the **airflow-prod-ssa-eda** namespace when changes are made to the **main** branch.
* Check the status of the build [here](https://console-openshift-console.apps.ssa-prod.20r1.p1.openshiftapps.com/k8s/ns/airflow-prod-ssa-eda/buildconfigs/qualtrics-ingestion/builds).
* The image will be accessible at this address `image-registry.openshift-image-registry.svc:5000/airflow-prod-ssa-eda/qualtrics-ingestion:latest` from the airflow `KubernetesPodOperator`. Please check `dag_qualtrics_ingestion.py` for an example airflow run.

### Create the image locally

**Note:** If using the devcontainer to develop code you would only 
want to do this to make sure your prod image works after you have your code 
is working in the dev container.
1. install [podman](https://podman.io/getting-started/installation) or [docker](https://docs.docker.com/get-docker/).
2. install [just](https://github.com/casey/just#packages)
3. run `$just podman-build` or `$just docker-build` to build the image.

### Run the image locally

**Note:** If using the devcontainer to develop code you would only 
want to do this to make sure your prod image works after you have your code 
is working in the dev container.
1. make a copy of `sample.env` and rename to `.env` and fill in missing fields.
2. run `$just podman-run` or `$just docker-run` to run the etl.

### Debugging locally

**Note:** If using the devcontainer you to develop code you would only 
want to do this to debug prod image issues that aren't present in your devcontainer.
This can happen because the way we install dependencies is different in prod and devcontainer.
1. make a copy of `sample.env` and rename to `.env` and fill in missing fields.
2. be sure to add any secrets that you have added to openshift to the `.env` file.
3. run `$just debug` to run the etl.
4. get into the container shell and use `vim` or other editor to put a `breakpoint()` in your code and manually run the `run_qualtrics_ingestion_with_date.sh` script and debug. **Remember:** changes made in the container are not reflected in the repo.
```
breakpoint help
    c         continue execution
    n         step to the next line within the same function
    s         step to the next line in this function or a called function
    q         quit the debugger/execution
    interact  start an interactive interpreter (using the code module) whose global namespace contains all the (global and local) names found in the current scope.
    ctrl+d    stop the interactive interpreter to continue execution.
```

### Running tests

1. Navigate to the root level of the project.
2. Run the command `just pytest` to run pytest a coverage check.
3. Continue to add additional tests to meet your projects requirements.

## Devops Architecture

![Architecture diagram](./docker-pattern.svg)