#!/bin/sh

BOOTNODE="enr:-Ku4QHqVeJ8PPICcWk1vSn_XcSkjOkNiTg6Fmii5j6vUQgvzMc9L1goFnLKgXqBJspJjIsB91LTOleFmyWWrFVATGngBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpC1MD8qAAAAAP__________gmlkgnY0gmlwhAMRHkWJc2VjcDI1NmsxoQKLVXFOhp2uX6jeT0DvvDpPcU8FWMjQdR4wMuORMhpX24N1ZHCCIyg"

WORKDIR="results"
echo $WORKDIR
#mkdir -p "$WORKDIR"
git diff > $WORKDIR/git.diff
git status > $WORKDIR/git.status
git describe --always > $WORKDIR/git.describe

cd "$WORKDIR"
../build/dcrawl --bootnode="$BOOTNODE" "$@"
