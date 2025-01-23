import json
import Pandas as pd

def lambda_handler(event, context):
    # TODO implement
    print(f"{event["message"]}")

    pooling_group_name_list = []
    res_pooling_group_list = [
        {
            "company_and_year": "NYK2024",
            "group_name"      : "group1",
            "imp_list"        : []
        },
        {
            "company_and_year": "NYK2024",
            "group_name"      : "group2",
            "imp_list"        : []
        },
                {
            "company_and_year": "NYK2024",
            "group_name"      : "group3",
            "imp_list"        : []
        }
    ]
    for i in range(len(res_pooling_group_list)):
        pooling_group_name_list.append(res_pooling_group_list[i]["group_name"])
    columns1 =["total_lng", "total_hfo", "total_lfo", "total_mdo", "total_mgo", "total_energy"]
    df = pd.DataFrame(index=pooling_group_name_list, columns = columns1)
    df

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
