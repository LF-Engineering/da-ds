# Cython image

Create a Cython image needed to compile pythond code from uuid.py to a static binary: `[DOCKER_USER=...] ./build.sh`

# Compilation

Do the actual compilation: `[DOCKER_USER=...] ./compile.sh`.

# TODO

The final binary *is* indeed static, but seems to be broken using `alpine` image, so this needs more investigation.

This is probably due to missing Python data files, that are needed by this static binary.
