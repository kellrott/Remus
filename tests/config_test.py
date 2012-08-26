

#REMOTE_SSH is for network testing. Will assume the following:
# - Password free SSH
# - The testing directory is mounted on NFS
# - The testing directory has the same path on both machines
# - Both machine have the same Python install path
REMOTE_SSH=None

DEFAULT_DB="file://data_dir"
DEFAULT_EXE="process"