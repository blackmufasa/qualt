set dotenv-load := true

registry-url := "default-route-openshift-image-registry.apps.ssa-prod.20r1.p1.openshiftapps.com"
namespace := "airflow-prod-ssa-eda"
image-name := "qualtrics-ingestion"


run-locally:
    poetry run splunk-py-trace python src/qualtrics_ingestion/trigger_qualtrics_ingestion.py -freq w

run-local-podman:
    podman build -t qualtrics-ingest:latest .  
    podman run -i -t qualtrics-ingest:latest poetry run splunk-py-trace python src/qualtrics_ingestion/trigger_qualtrics_ingestion.py -freq w

local-build-and-push: 
    
    buildah --storage-driver=vfs push --tls-verify=false localhost/test:latest "docker://{{registry-url}}/{{namespace}}/{{image-name}}:local-test"


# oc-deploy:
#     #!/usr/bin/env bash
#     set -euxo pipefail
#     if ! command -v oc &> /dev/null; then
#         echo "oc could not be found"
#         echo "install oc https://docs.openshift.com/container-platform/4.8/cli_reference/openshift_cli/getting-started-cli.html#installing-openshift-cli"
#         exit
#     fi


#     if oc whoami; then
#         if oc status | grep -q 'airflow-prod-ssa-eda'; then
#             echo "oc is logged in and in airflow-prod-ssa-eda namespace"
#         else
#             echo "oc is logged in but is not in the correct namespace"
#             echo "run oc project airflow-prod-ssa-eda to switch to the correct namespace."
#             exit
#         fi
#     else
#         echo "oc is not logged in"
#         echo "find login command here https://oauth-openshift.apps.ssa-prod.20r1.p1.openshiftapps.com/oauth/token/request"
#         exit
#     fi
    
    
#     oc apply -f oc-build/

# build-machine-init:
#     podman machine init

# build-machine-start:
#     podman machine start

# build-machine-stop:
#     podman machine stop

# build-machine-restart:
#     podman machine stop
#     podman machine start

# podman-build:
#     podman build -t qualtrics-ingestion --label qualtrics-ingestion . 

# podman-run:
#     #!/usr/bin/env bash
#     if podman ps | grep 'qualtrics-ingestion'; then
#         podman stop qualtrics-ingestion
#     fi
#     podman run -i \
#     --env-file=.env \
#     qualtrics-ingestion

# podman-debug:
#     #!/usr/bin/env bash
#     if podman ps | grep 'qualtrics-ingestion'; then
#         podman stop qualtrics-ingestion
#     fi
#     podman run -i \
#     --name qualtrics-ingestion \
#     --env-file=.env \
#     qualtrics-ingestion /bin/bash
#     podman exec -it qualtrics-ingestion /bin/bash

# podman-image-cleanup:
#     podman image prune --force --filter='label=qualtrics-ingestion'

# docker-build:
#     docker build -t qualtrics-ingestion --label qualtrics-ingestion -f Containerfile . 

# docker-run:
#     #!/usr/bin/env bash
#     if docker ps | grep 'qualtrics-ingestion'; then
#         docker stop qualtrics-ingestion
#     fi
#     docker run -i \
#     --env-file=.env \
#     qualtrics-ingestion

# docker-debug:
#     #!/usr/bin/env bash
#     if docker ps | grep 'qualtrics-ingestion'; then
#         docker stop qualtrics-ingestion
#     fi
#     docker run -i \
#     --name qualtrics-ingestion \
#     --env-file=.env \
#     qualtrics-ingestion /bin/bash
#     docker exec -it qualtrics-ingestion /bin/bash

# docker-image-cleanup:
#     docker image prune --force --filter='label=qualtrics-ingestion'

# pytest:
#     #!/usr/bin/env bash
#     cd ./src
#     poetry install
#     poetry update
#     poetry run pytest ../test/ --cov 
