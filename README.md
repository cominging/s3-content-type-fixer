# S3 Content Type Fixer (boto3) #

This script will scan an S3 bucket to find files with bad content-types and
fix them.

To run the script, first install the dependencies:

    pip install -r requirements.txt

Then run the fixer:

    python s3_content_type_fixer.py --access-key <your AWS access key> --secret-key <your AWS secret key> --bucket <your S3 bucket>
