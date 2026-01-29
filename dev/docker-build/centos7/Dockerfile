#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

FROM centos:7

# install common tools
RUN echo "sslverify=false" >> /etc/yum.conf
RUN sed -i "s/mirror.centos.org/vault.centos.org/g" /etc/yum.repos.d/*.repo
RUN sed -i "s/^#.*baseurl=http/baseurl=https/g" /etc/yum.repos.d/*.repo
RUN sed -i "s/^mirrorlist/#mirrorlist/g" /etc/yum.repos.d/*.repo
RUN yum update -y
RUN yum install -y centos-release-scl epel-release
RUN sed -i "s/mirror.centos.org/vault.centos.org/g" /etc/yum.repos.d/*.repo
RUN sed -i "s/^#.*baseurl=http/baseurl=https/g" /etc/yum.repos.d/*.repo
RUN sed -i "s/^mirrorlist/#mirrorlist/g" /etc/yum.repos.d/*.repo
RUN sed -i "s|^metalink=|#metalink=|g" /etc/yum.repos.d/epel.repo
RUN sed -i "s|https://download.fedoraproject.org/pub/epel|https://archives.fedoraproject.org/pub/archive/epel|g" /etc/yum.repos.d/epel.repo
RUN yum clean all && yum makecache
RUN yum install -y libzip unzip openssl-devel

# install gcc-11
RUN yum install -y devtoolset-11-gcc devtoolset-11-gcc-c++
RUN echo '. /opt/rh/devtoolset-11/enable' >> ~/.bashrc

# install rust nightly toolchain
RUN curl https://sh.rustup.rs > /rustup-init
RUN chmod +x /rustup-init
RUN /rustup-init -y --default-toolchain nightly-2025-05-09-x86_64-unknown-linux-gnu
ENV PATH="/root/.cargo/bin:${PATH}"
RUN rustup target add x86_64-unknown-linux-gnu

# install java
RUN echo "Installing Java 17 (Cache Buster)" && \
    curl -L -o /tmp/java17.rpm https://corretto.aws/downloads/latest/amazon-corretto-17-x64-linux-jdk.rpm && \
    yum localinstall -y /tmp/java17.rpm && \
    rm /tmp/java17.rpm
ENV JAVA_HOME="/usr/lib/jvm/java-17-amazon-corretto"
ENV PATH="${JAVA_HOME}/bin:${PATH}"
RUN echo "export JAVA_HOME=${JAVA_HOME}" >> ~/.bashrc
RUN echo "export PATH=${JAVA_HOME}/bin:\$PATH" >> ~/.bashrc

ENV RUSTFLAGS="-C target-cpu=nehalem"
RUN echo 'export RUSTFLAGS="-C target-cpu=nehalem"' >> ~/.bashrc

RUN mkdir -p /root/.cargo && \
    echo '[build]' > /root/.cargo/config && \
    echo 'rustflags = ["-C", "target-cpu=nehalem"]' >> /root/.cargo/config && \
    cp /root/.cargo/config /root/.cargo/config.toml

RUN cat /root/.cargo/config
