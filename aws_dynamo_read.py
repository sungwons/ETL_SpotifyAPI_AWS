import sys
import boto3
from boto3.dynamodb.conditions import Key, Attr
import logging


def main():

    # AWS - DynamoDB Connection
    # For Credential Issue, AWS CLI (aws configure) - IAM Setting needed to cennect table
    try:
        dynamodb = boto3.resource('dynamodb', region_name='us-east-2', endpoint_url="http://dynamodb.us-east-2.amazonaws.com")
    except:
        logging.error("Couldn't connect to dynamodb.")
        sys.exit(1)

    table = dynamodb.Table('top_tracks')

    # Read DynamoDB - https://boto3.amazonaws.com/v1/documentation/api/latest/guide/dynamodb.html
    response = table.query(
        KeyConditionExpression=Key('artist_id').eq('0FI0kxP0BWurTz8cB8BBug'),
        FilterExpression=Attr('popularity').gt(45)
    )

    print(len(response['Items']))
    


if __name__ == "__main__":
    main()