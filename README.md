# 2-way S3 Replication

This script only support replication from 1 source account_id to multiple account_id

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install boto3.

```bash
pip install boto3
```

## Usage
Input value for this variable in `config.py`.

example:
```python
### replication role name on source account
src_role_replication_name=""

### replication role name on destination account
dst_role_replication_name=""

### batch role name
batch_replication_role_name=""

### source account id 
src_account_id = "11111111"

### region for bucket
src_region="ap-southeast-1"

### =region for destination bucket
dst_region="ap-southeast-3"

### your csv file name
csv_file_name="bucket.csv
```

for csv template, please look at `template.csv`.

run this command `python3 config.py` to start the replication.

## Features
- Assume role
- get bucket Cors
- get bucket tagging
- Encryption supported ('SSEAlgorithm': 'AES256')
- get bucket policy
- get public access
- ACL


## Contributing

Pull requests are welcome. For major changes, please open an issue first
to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License

[MIT](https://choosealicense.com/licenses/mit/)
