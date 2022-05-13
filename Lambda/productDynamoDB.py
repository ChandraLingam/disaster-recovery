import boto3
import json
import decimal
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
import uuid
import sys
from datetime import datetime


# Identify the profile to use for this session
session = boto3.Session()

# Acquire DynamoDB resource
dynamodb = session.resource('dynamodb')

# Table
productTable = dynamodb.Table("product")

# Helper class to convert a DynamoDB item to JSON.
# The DecimalEncoder class is used to print out numbers stored using the 
# Decimal class. The Boto SDK uses the Decimal class to hold 
# Amazon DynamoDB number values.
# https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.Python.03.html
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

# Get Products
def get_products(limit=100, lastEvaluatedKey=None):
    print ("Reading all available products")
    
    if lastEvaluatedKey is None:
        # returns the first page
        response = productTable.scan(Limit=limit)
    else:
        # continue from previous page
        response = productTable.scan(Limit=limit, ExclusiveStartKey=lastEvaluatedKey)

    result = {}

    if 'LastEvaluatedKey' in response:
        result['LastEvaluatedKey'] = response['LastEvaluatedKey'] 

    result['Items'] = response['Items']

    return result

# Get a specific product
def get_product(productId):
    print (f"Reading product with Id:{productId}")

    response = productTable.get_item(Key={
                'product_id': productId
            })

    print(json.dumps(response['Item'], indent=4, cls=DecimalEncoder))
    
    return response['Item']
    
# add a product
def add_product(productDetail):
    '''
    Template - product_id is auto generated
    {
        "product_category": "computer",
        "product_title": "Ergo Mouse"
    }
    '''
    # a best practice in DynamoDB is to use a random value
    # for partition key - it ensures items are distributed evenly 
    # across available partitions
    # uuid is a unique id generator
    
    uniqueID = str(uuid.uuid4())
    print (f'Adding Item with product id {uniqueID}, {productDetail}')

    response = productTable.put_item(
        Item={
            'product_id': uniqueID,
            'product_category': productDetail['product_category'],
            'product_title': productDetail['product_title'],
            'sum_rating': decimal.Decimal(0),
            'count_rating': decimal.Decimal(0)
            })

    return uniqueID
    
# update a product
def update_product(productDetail):
    '''
    Template
    {
        "product_id" : "uuid of the product"
        "product_category": "updated category",
        "product_title": "updated title"
    }
    '''
    print (f'Updating Item with product id {productDetail["product_id"]}')

    response = productTable.update_item(
        Key={
            'product_id': productDetail['product_id']
            },
        UpdateExpression="set product_category = :category, product_title = :title",
        ExpressionAttributeValues={
        ':category': productDetail["product_category"],
        ':title': productDetail["product_title"]},
        ConditionExpression='attribute_exists(product_id)',
        ReturnValues="NONE")

    return productDetail['product_id']        
    
# delete a product
def delete_product(productId):
    print (f"Deleting product: {productId}")

    response = productTable.delete_item(Key={
                'product_id': productId
            })

    print(json.dumps(response, indent=4, cls=DecimalEncoder))  
    return productId
    
    
def lambda_handler(event, context):
    try:
        # print("Received event: " + json.dumps(event, indent=2))
    
        # event["pathParameters"]["id"]
        # event["routeKey"]
        # event["body"]
        
        if 'routeKey' in event:
            print("**routeKey attribute value is:")
            print (event['routeKey'])
            
            limit = 100 # number of matches to return in a page
            lastEvaluatedKey = None # get next set of matching items from this key
            rating = 0 # filter by review rating
            
            if event['routeKey'] == "GET /products":
                # return products
                if 'queryStringParameters' in event:
                    if 'limit' in event['queryStringParameters']:
                        limit = int(event['queryStringParameters']['limit'])
                    
                    if 'LastEvaluatedKey' in event['queryStringParameters']:
                        lastEvaluatedKey = json.loads(event['queryStringParameters']['LastEvaluatedKey'])
                
                return {
                    'statusCode': 200,
                    'body': json.dumps(get_products(limit, lastEvaluatedKey), indent=4, cls=DecimalEncoder)
                    }
                    
            if event['routeKey'] == "GET /products/{productId}":
                # return a specific product
                return {
                    'statusCode': 200,
                    'body': json.dumps(get_product(event['pathParameters']['productId']), indent=4, cls=DecimalEncoder)
                    }
            
            if event['routeKey'] == "POST /products":
                # add a new product
                body = json.loads(event['body'])
                productId = add_product(body)
                
                return {
                    'statusCode': 200,
                    'body': json.dumps(f"POST item {productId}")
                    }
    
            if event['routeKey'] == "PUT /products":
                # update an existing product
                body = json.loads(event['body'])
                productId = update_product(body)
                
                return {
                    'statusCode': 200,
                    'body': json.dumps(f"PUT item {productId}")
                    }
        
            if event['routeKey'] == "DELETE /products/{productId}":
                # delete a product
                productId = delete_product(event['pathParameters']['productId'])
                return {
                    'statusCode': 200,
                    'body': json.dumps(f"DELETE item {productId}")
                    }             
                            
            return {
                'statusCode': 200,
                'body': json.dumps(f"NO ACTION for routeKey is {event['routeKey']}")
                }
        else:
            print("**No routeKey attribute")
            
            return {
                'statusCode': 200,
                'body': json.dumps("Hellow from Lambda - no routeKey found")
                }
    except:
        # Get error details
        exc_type, exc_value, exc_traceback = sys.exc_info()
        
        print (exc_type, exc_value, exc_traceback)
        
        return {
            'statusCode': 400,
            'body': json.dumps(str(exc_type) + " " + str(exc_value)),
            'headers': {'Content-Type': 'application/json',}
            }

