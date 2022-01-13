# S3Synchrony

This package provides a service for synchronizing file creations, deletions, and modifications across users on an AWS S3 prefix. Support also exists for easily expanding to other database systems.

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install s3synchrony.

```bash
pip install s3synchrony
```

## Usage

```python
import s3synchrony as s3s

# returns a list of data platforms currently supported
s3s.get_supported_platforms()

# prompts user to synchronize all detected changes in the local and remote repositories
s3s.smart_sync(platform="S3", aws_bkt="bucket_name", aws_prfx="prfx_path")

# prompts user to remove all synchronization support on the local and remote repositories
s3s.reset_all(platform="S3", aws_bkt="bucket_name", aws_prfx="prfx_path")
```


## License
[GNU GPLv3](https://www.gnu.org/licenses/)