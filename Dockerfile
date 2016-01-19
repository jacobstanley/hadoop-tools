FROM jystic/centos6-ghc7.10.1

# Dependencies
RUN yum install -y epel-release rpmdevtools tar
RUN yum install -y libgsasl-devel

# Update cabal
RUN cabal update

# Add .cabal file
ADD ./hadoop-tools.cabal /src/hadoop-tools/hadoop-tools.cabal

# Docker will cache this command as a layer, freeing us up to
# modify source code without re-installing dependencies
RUN cd /src/hadoop-tools && (mafia build || exit 0)

# Add and install application code
ADD . /src/hadoop-tools
RUN cd /src/hadoop-tools && mafia build

# Create RPM Tree
RUN rpmdev-setuptree

# Bundle "sources" in to a tarball
WORKDIR /root/rpmbuild
RUN mkdir -p hadoop-tools-0.7/usr/bin/
RUN mkdir -p hadoop-tools-0.7/etc/bash_completion.d/
RUN install -m 755 /src/hadoop-tools/dist/build/hh/hh   hadoop-tools-0.7/usr/bin/
RUN install -m 755 /src/hadoop-tools/hh-completion.bash hadoop-tools-0.7/etc/bash_completion.d/
RUN tar -zcvf SOURCES/hadoop-tools-0.7.tar.gz           hadoop-tools-0.7/

# Build RPM
RUN cp /src/hadoop-tools/hadoop-tools.spec SPECS/
RUN rpmbuild -ba                           SPECS/hadoop-tools.spec

# Install RPM
RUN yum install -y RPMS/x86_64/*.rpm

# Default Command for Container
CMD ["hh"]
