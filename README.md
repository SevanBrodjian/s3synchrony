# S3Synchrony

_Created by Sevan Brodjian for Ameren at the Innovation Center @ UIUC_

This package provides a service for synchronizing file creations, deletions, and modifications across users on an AWS S3 prefix. Support also exists for easily expanding to other database systems.

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install s3synchrony.

```bash
pip install s3synchrony
```

## Requirements

S3Synchrony relies on Python 3 and the following packages to operate:

- hashlib
- datetime
- pandas
- boto3
- botocore
- pyperclip

## Usage

S3Synchrony comes with three primary functions, which can be called as follows:

```python
import s3synchrony as s3s

# returns a list of data platforms currently supported
s3s.get_supported_platforms()

# prompts user to synchronize all detected changes in the local and remote repositories
s3s.smart_sync(platform="S3", aws_bkt="bucket_name", aws_prfx="prfx_path")

# prompts user to remove all synchronization support on the local and remote repositories
s3s.reset_all(platform="S3", aws_bkt="bucket_name", aws_prfx="prfx_path")
```

## The Data Folder

When using S3Synchrony, you are synchronizing all of the data stored in a local directory with the data stored in an S3 directory. The S3 directory is referenced through both an AWS bucket, an AWS prefix, and the necessary credentials to access said prefix. The local directory to be used can be a relative or full path, and by default will be a subdirectory named "Data" stored in the same working directory.

- Project Folder
  - Data
  - code, etc.

## smart_sync

```python
def smart_sync(platform="S3", **kwargs):
    """Perform all necessary steps to synchronize a local repository   with a remote repo.

    Notes:
    Keyword arguments are dependent on platform selection.
    """


    if(platform in _supported_platforms):
        connection = _supported_platforms[platform](**kwargs)
    else:
        connection = baseconn.DataPlatformConnection(**kwargs)

    connection.intro_message()
    connection.establish_connection()
    connection.synchronize()
    connection.close_message()
```

The smart_sync function is the premier work of this package, and will perform all of the data synchronization for you. This function will check the passed platform name, and reference a self-contained list of supported platforms to instantiate the proper class. This list of supported platforms can be accessed via a call to get_supported_platforms().

Each connection type will require a different set of keyword arguments. For S3, the minimum arguments are "aws_bkt" and "aws_prfx". Please check the class docstrings for each connection type for more information.

All platform classes should be children of the DataPlatformConnection class which is an interface will all necessary public functions. For S3, a folder named .S3 will be created within your data folder. This .S3 folder will contain CSVs used for monitoring data changes and text files for storing small bits of information.

- **versions.csv:** Contains the state of data stored on s3
- **versionsLocal.csv:** Contains the state of data stored locally
- **deletedS3.csv:** Contains all files deleted from S3
- **deletedLocal.csv:** Contains all files deleted locally
- **ignores3.txt:** Contains a list of file paths to be ignored entirely
- **user_name.txt:** Contains the name attached to your file modifications
- **aws.txt:** Contains credentials used to access the AWS prefix

Using these CSVs, S3Synchrony can determine what files you have newly created, deleted, and modified. It will then prompt you to upload these changes to S3. Once you have done so, it will upload new CSVs as needed. After downloading these new CSVs, your collaborative peers will be prompted to download your own changes as well as upload their own.

In addition, a tmp folder will be utilised within the .S3 folder. This tmp folder contains downloaded files from S3 that are used to compute certain CSVs.

## Deletions

When deleting files, the user will be prompted to confirm their deletions. Files that are deleted locally will simply be removed. Files deleted from S3, however, will simply be moved into a "deleted" subfolder of the .S3 folder on S3.

## Logs

When there are any issues with a file being uploaded or downloaded, an error message will be printed and that file will be skipped. A log will then be created a saved locally inside of the "logs" subfolder of the local .S3 folder.

## reset_all

```python
def reset_all(platform="S3", **kwargs):
    """Reset local and remote directories to original state.

    Notes:
    Keyword arguments are dependent on platform selection.
    """

    if(platform in _supported_platforms):
        connection = _supported_platforms[platform](**kwargs)
    else:
        connection = baseconn.DataPlatformConnection(**kwargs)

    connection.intro_message()
    connection.establish_connection()
    if connection.reset_confirm():
        connection.reset_local()
        connection.reset_remote()
    connection.close_message()
```

Resetting all S3Synchrony services is as simple as deleting the .S3 folders contained locally and on S3. Once these are deleted, synchronization cannot occur until they are recreated, which can be done by simply making a new call to S3Synchrony.

Before resetting, however, a call to reset_confirm **must** occur. The user will then be prompted to confirm that they would like their .S3 folders removed.

## License

[GNU GPLv3](https://www.gnu.org/licenses/)
