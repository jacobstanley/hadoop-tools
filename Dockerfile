FROM jystic/centos6-ghc7.8.4

## Update cabal
RUN cabal update

## Add .cabal file
ADD ./hadoop-tools.cabal /opt/hadoop-tools/hadoop-tools.cabal

# Docker will cache this command as a layer, freeing us up to
# modify source code without re-installing dependencies
RUN cd /opt/hadoop-tools && cabal install --only-dependencies -j4

# Add and install application code
ADD . /opt/hadoop-tools
RUN cd /opt/hadoop-tools && cabal install

# Add installed cabal executables to PATH
ENV PATH /root/.cabal/bin:$PATH

# Default command for container
CMD ["hh"]
