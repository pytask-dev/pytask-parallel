# Remote backends

There are a couple of things you need to know when using backends that launch workers
remotely, meaning not on your machine.

## Cross-platform support

Issue: {issue}`102`.

Currently, it is not possible to run tasks in a remote environment that has a different
OS than your local system. The reason is that when pytask sends data to the remote
worker, the data contains path objects, {class}`pathlib.WindowsPath` or
{class}`pathlib.PosixPath`, which cannot be unpickled on a different system.

In general, remote machines are Unix machines which means people running Unix systems
themselves like Linux and MacOS should have no problems.

Windows users on the other hand should use the
[WSL (Windows Subsystem for Linux)](https://learn.microsoft.com/en-us/windows/wsl/about)
to run their projects.

## Local files

In most projects you are using local paths to refer to dependencies and products of your
tasks. This becomes an interesting problem with remote workers since your local files
are not necessarily available in the remote machine.

pytask-parallel does its best to sync files before the execution to the worker, so you
can run your tasks locally and remotely without changing a thing.

In case you create a file on the remote machine, the product will be synced back to your
local machine as well.

It is still necessary to know that the remote paths are temporary files that share the
same file extension as the local file, but the name and path will be different. Do not
rely on them.

Another way to circumvent the problem is to first define a local task that stores all
your necessary files in a remote storage like S3. In the remaining tasks, you can then
use paths pointing to the bucket instead of the local machine. See the
[guide on remote files](https://tinyurl.com/pytask-remote) for more explanations.
