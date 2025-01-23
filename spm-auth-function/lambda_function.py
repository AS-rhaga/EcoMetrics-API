from jwtauthorizer import authorizer, create_response


def lambda_handler(event,context):

    auth = authorizer(event)

    if auth:
        json_response = create_response(event["methodArn"],"Allow")
    else:
        json_response = create_response(event["methodArn"])
    return json_response

