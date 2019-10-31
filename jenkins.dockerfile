FROM centos:7

RUN curl -s https://mirror.go-repo.io/centos/go-repo.repo > /etc/yum.repos.d/go-repo.repo && \
    curl -s https://www.apache.org/dist/bigtop/stable/repos/centos7/bigtop.repo > /etc/yum.repos.d/bigtop.repo && \
    curl -s https://downloads.mariadb.com/MariaDB/mariadb_repo_setup | bash && \
    yum install -y epel-release && \
    yum install -y \
        gcc gcc-c++ make git \
        postgresql-devel \
        librabbitmq-devel \
        mariadb-client \
        python \
        python-virtualenv \
        java \
        golang \
        mariadb-server \
        redis \
        zookeeper
ENV GOPATH "/opt/gopath"
RUN go get -v -u github.com/client9/misspell/cmd/misspell && \
    virtualenv /opt/venv && \
    /opt/venv/bin/pip install -U pip setuptools wheel
RUN yum install -y ShellCheck

ENV VIRTUAL_ENV "/opt/venv"
ENV PATH "/opt/venv/bin:/opt/gopath/bin:/usr/local/sbin:/usr/sbin:/sbin:/usr/local/bin:/usr/bin:/bin:/usr/libexec"

RUN useradd -u 1060 -m jenkins && \
    printf 'unset JAVA_HOME\n' > /usr/lib/zookeeper/conf/java.env && \
    mkdir -p \
        /var/{lib,log,run}/{mysql,mariadb,redis,zookeeper} && \
    chown -R jenkins:jenkins \
        /var/{lib,log,run}/{mysql,mariadb,redis,zookeeper} \
        /opt/{venv,gopath}
