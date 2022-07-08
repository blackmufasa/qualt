FROM registry.access.redhat.com/ubi8/python-39

LABEL maintainer="eda-bits@redhat.com"

LABEL io.k8s.description="This ETL Extracts Transforms and Loads." \
    io.k8s.display-name="Qualtrics Ingestion" \
    io.openshift.tags="etl-template,dpaas"

ARG JAVA_VERSION
ARG TEIID_VERSION
ARG OC_VERSION
ARG DEVCONTAINER
ARG FREQ

ENV JAVA_VERSION=${JAVA_VERSION:-1.8.0}
ENV TEIID_VERSION=${TEIID_VERSION:-16.0.0}
ENV OC_VERSION=${OC_VERSION:-stable-4.9}
ENV DEVCONTAINER=${DEVCONTAINER:-0}
ENV FREQ=${FREQ:-"-freq d"}
USER root

ENV JAVA_HOME=/usr/lib/jvm/jre
ENV TEIID_JAR_PATH="/tmp/teiid-${TEIID_VERSION}-jdbc.jar"
ENV POETRY_HOME="/opt/poetry"
ENV POETRY_VERSION=1.1.12
ENV POETRY_VIRTUALENVS_IN_PROJECT=true
ENV APP_FILE="qualtrics_ingestion/trigger_qualtrics_ingestion.py"

RUN if [ "$DEVCONTAINER" == "1" ]; then \
    # install binaries for development and deployment within a devcontainer
        curl --proto '=https' --tlsv1.2 -sSf https://just.systems/install.sh | \
            bash -s -- --to /opt/app-root/bin && \
        curl -s https://mirror.openshift.com/pub/openshift-v4/clients/ocp/$OC_VERSION/openshift-client-linux.tar.gz | \
            tar zxvf - -C /opt/app-root/bin oc; \
    fi && \
    # install java to enable querying using jdbc drivers
    yum -y install java-$JAVA_VERSION-openjdk && \
    yum clean all && \
    # install teiid driver for querying JDV
    wget https://oss.sonatype.org/service/local/repositories/releases/content/org/teiid/teiid/$TEIID_VERSION/teiid-$TEIID_VERSION-jdbc.jar -P /tmp/ && \
    # install Red Hat IT Root Certificate for enabling access to internal IT signed URLs like JDV or gitlab
    wget https://password.corp.redhat.com/RH-IT-Root-CA.crt -P /etc/pki/ca-trust/source/anchors/ && \
    keytool -import -noprompt -keystore /etc/pki/java/cacerts -file /etc/pki/ca-trust/source/anchors/RH-IT-Root-CA.crt -alias RH-IT-Root-CA -storepass changeit && \
    update-ca-trust enable && \
    update-ca-trust extract && \
    # install poetry so that we can export poetry dependencies to requirements.txt
    curl -sSL https://install.python-poetry.org | python3 -

ENV PATH="/opt/poetry/bin:${PATH}"

# enable python requests package to access internal IT signed URLs
ENV REQUESTS_CA_BUNDLE=/etc/pki/tls/certs/ca-bundle.crt

# Add application sources to a directory that the assemble script expects them
# and set permissions so that the container runs without root access
USER 0
COPY src /tmp/src
COPY pyproject.toml /tmp/src
COPY poetry.lock /tmp/src

RUN cd /tmp/src && \
    if [ "$DEVCONTAINER" == "1" ]; then \
    # replace the default bash startup script to activate poetry virtual environment instead of venv.
    sed -i 's/source \/opt\/app-root\/bin\/activate/\/opt\/poetry\/bin\/poetry shell/' \
        ${APP_ROOT}/etc/scl_enable; \
    else \
    # export poetry dependencies to requirements.txt to leverage built in
    # s2i dependency installer in /usr/libexec/s2i/assemble script
    poetry export -f requirements.txt -o requirements.txt --no-interaction --no-ansi --without-hashes && \ 
    # fix s2i run script
    sed -i 's/maybe_run_in_init_wrapper python \"\$APP_FILE\"/maybe_run_in_init_wrapper python \"\$APP_FILE\" \${FREQ}/' /usr/libexec/s2i/run && \
    # fix permissions on src folder
    /usr/bin/fix-permissions /tmp/src ; \
    fi

USER 1001

# Install the dependencies if in production
RUN if [ "$DEVCONTAINER" != "1" ]; then \
    /usr/libexec/s2i/assemble; \
    fi

# Set the default command for the resulting image
CMD if [ "$DEVCONTAINER" != "1" ]; then \
    /usr/libexec/s2i/run; \
    fi

