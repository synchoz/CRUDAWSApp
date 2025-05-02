import json
import boto3
import os
import uuid
from boto3.dynamodb.conditions import Attr

def findMoviesByTitle(title):
    response = table.scan(
        FilterExpression=Attr('title').eq(title)
    )
    return response.get("Items", [])

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['MOVIES_TABLE'])

def lambda_handler(event, context):
    if event['httpMethod'] == "GET":
        title = event['queryStringParameters']['title']
        items = findMoviesByTitle(title)

        if not items:
            return { "statusCode": 404, 'body': f"No movie was found with title: {title}" }
        
        return { "statusCode": 200, 'body': json.dumps(items) }
    elif event['httpMethod'] == "POST":
        movie = json.loads(event['body'])
        params = {
            'id': str(uuid.uuid4()),
            'title': movie['title'],
            'description': movie['description'],
            'releaseDate': movie['releaseDate'],
            'durationX': movie['durationX']
        }

        response = table.put_item(Item=params)
        print(response)
        return { "statusCode": 200, "body": f"movie {movie['title']} Added" }
    elif event['httpMethod'] == "DELETE":
        title = event['queryStringParameters']['title']
        items = findMoviesByTitle(title)

        if not items:
            return { "statusCode": 404, 'body': f"No movie was found with title: {title}" }
        
        deletedIds = []

        for item in items:
            response = table.delete_item(Key={'id': item['id']})
            deletedIds.append(item['id'])
            print(response)
            
        return { 'statusCode': 200, 'body': json.dumps({'message': f'deleted {len(deletedIds)} Movies, with title: {title} ', 'deleted ids': deletedIds})} 
    elif event['httpMethod'] == "PUT":
        movie = json.loads(event['body'])
        title = event['queryStringParameters']['title']
        items = findMoviesByTitle(title)

        if not items:
            return { "statusCode": 404, 'body': f"No movie was found with title: {title}" }
        
        #need validation for body to include description/duration/releaseDate
        updatedIds = []

        for item in items:
            print(item)
            response = table.update_item(
                Key={'id': item['id']},
                UpdateExpression="SET description = :newDescription, releaseDate = :newReleaseDate, durationX = :newDurationX",
                ExpressionAttributeValues={
                    ':newDescription': movie.get('description', item['description']),
                    ':newReleaseDate': movie.get('releaseDate', item['releaseDate']),
                    ':newDurationX': movie.get('durationX', item['durationX'])
                },
                ReturnValues="ALL_NEW"
            )
            updatedIds.append(item['id'])
            print(response)
            
        return { 'statusCode': 200, 'body': json.dumps({'message': f'updated {len(updatedIds)} Movies, with title: {title} ', 'updated ids': updatedIds})} 
    
    return { "statusCode": 400, "body": "Unsupported Method"}

