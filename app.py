import csv
import os
import json
import pymongo

import pandas as pd
import re
from flask import Flask, jsonify, request
from flask_cors import CORS
from datetime import datetime, timezone, timedelta
from bson.json_util import dumps


app = Flask(__name__)


CORS(app, supports_credentials=True)
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["Transactions"]

DB_data_compute = db["DB_data_compute"]
DB_get_transactions = db["DB_get_transactions"]
DB_search = db["DB_search"]
DB_Dapp = db["DB_Dapp"]
DB_IndirectTransfer = db["DB_IndirectTransfer"]


@app.route('/')
def hello_world():
    return 'Hello World!'


# 交易哈希的处理
def format_hash(hash_str):
    # 对 hash 列进行对称显示处理
    mid = len(hash_str) // 2
    formatted_hash = hash_str[:mid] + '\n' + hash_str[mid:]
    return formatted_hash


# 查找与地址相关的交易
def find_associated_transactions(address, df):
    # 检查地址是否在'from'或'to'列中
    filtered_df = df[(df['from'] == address) | (df['to'] == address)]
    response_data = []

    for _, row in filtered_df.iterrows():
        associated_account = row['to'] if row['from'] == address else row['from']
        response_data.append({
            'associated_account': associated_account,
            'HeuristicAssociateRuleNum': row['HeuristicAssociateRuleNum'],
            'asset_pool': row['asset_pool']
        })

    return response_data


# 查找与地址相关的交易
def find_associated_crosschaintx(address, df):
    # 检查地址是否在'from'或'to'列中
    filtered_df = df[(df['from'] == address) | (df['to'] == address)]
    response_data = []

    for _, row in filtered_df.iterrows():
        associated_account = row['to'] if row['from'] == address else row['from']
        response_data.append({
            'associated_account': associated_account,
            'HeuristicAssociateRuleNum': row['HeuristicAssociateRuleNum'],
            'ContractAddress': row['ContractAddress']
        })

    return response_data


# 查找与地址相关的交易
def find_associated_ERC20TX(address, df):
    # 检查地址是否在'from'或'to'列中
    filtered_df = df[(df['from'] == address) | (df['to'] == address)]
    response_data = []

    for _, row in filtered_df.iterrows():
        associated_account = row['to'] if row['from'] == address else row['from']
        response_data.append({
            'associated_account': associated_account,
            'HeuristicAssociateRuleNum': row['HeuristicAssociateRuleNum'],
            'hash': row['hash']
        })

    return response_data


client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["Transactions"]
DB_IndirectTransfer = db["DB_IndirectTransfer"]


# @app.route('/GetHeuristicRulesⅠ_IndirectTransfer', methods=['GET'])
# def GetHeuristicRulesⅠ_IndirectTransfer():
#     address = request.args.get('address')
#     if not address:
#         app.logger.error('No address provided')
#         return jsonify([]), 400
#
#     try:
#         # 创建 MongoDB 查询
#         query = {'$or': [{'from': address}, {'to': address}]}
#         cursor = DB_IndirectTransfer.find(query)
#         # 将查询结果转换为列表
#         transactions_list = list(cursor)
#         # 如果没有找到相关的交易，返回404
#         if not transactions_list:
#             return jsonify({'message': 'No associated transactions found'}), 404
#
#         # 使用 dumps 方法将结果转换为 JSON 字符串，然后再转换为 JSON 对象
#         response_data = json.loads(dumps(transactions_list))
#         for transaction in response_data:
#             transaction.pop('_id', None)  # 移除没用的字段MongoDB _id
#
#         # 返回响应数据
#         return jsonify({"data": {"transactions": response_data}, "code": 20000})
#     except Exception as e:
#         app.logger.error('Error retrieving data from MongoDB')
#         return jsonify({'error': str(e)}), 500


# 支持接收输入的检索类型：交易哈希或地址
@app.route('/GetHeuristicRulesⅠ_IndirectTransfer', methods=['GET'])
def GetHeuristicRulesⅠ_IndirectTransfer():
    search_input = request.args.get('input')
    search_type = request.args.get('type')

    if not search_input:
        app.logger.error('No search input provided')
        return jsonify([]), 400

    try:
        # 创建 MongoDB 查询
        if search_type == 'address':
            query = {'$or': [{'from': search_input}, {'to': search_input}]}
        elif search_type == 'hash':
            query = {'hash': search_input}
        else:
            return jsonify({'error': 'Invalid search type'}), 400

        cursor = DB_IndirectTransfer.find(query)
        # 将查询结果转换为列表
        transactions_list = list(cursor)

        # 如果没有找到相关的交易，返回404
        if not transactions_list:
            return jsonify({'message': 'Sorry, No associated transactions found!/(ㄒoㄒ)/~~'})

        # 准备响应数据
        response_data = []
        for transaction in transactions_list:
            # 移除 MongoDB 自动生成的 _id 字段
            transaction.pop('_id', None)

            # 处理地址搜索逻辑
            if search_type == 'address':
                # 检查 address 是否同时出现在 from 和 to 字段中
                if search_input == transaction['from'] and search_input == transaction['to']:
                    # 地址同时出现在 from 和 to，添加双向关联账户
                    associated_accounts = [transaction['from'], transaction['to']]
                elif search_input == transaction['from']:
                    # 地址只出现在 from，添加 to 作为关联账户
                    associated_accounts = [transaction['to']]
                elif search_input == transaction['to']:
                    # 地址只出现在 to，添加 from 作为关联账户
                    associated_accounts = [transaction['from']]
                else:
                    # 如果地址不在 from 或 to 中，则跳过此交易
                    continue

                # 构建每个交易的响应数据
                for account in associated_accounts:
                    # Format 'hash'
                    transaction['hash'] = format_hash(transaction['hash'])
                    response_data.append({
                        'associated_account': account,
                        'asset_pool': transaction['asset_pool'],
                        'hash': transaction['hash'],
                        'HeuristicAssociateRuleNum': transaction['HeuristicAssociateRuleNum']
                    })

            elif search_type == 'hash':
                # 处理哈希搜索逻辑
                response_data.append({
                    'from': transaction.get('from', ''),
                    'to': transaction.get('to', ''),
                    'asset_pool': transaction.get('asset_pool', ''),
                    # 'hash': search_input,  # 哈希值与搜索输入相同
                    'HeuristicAssociateRuleNum': transaction.get('HeuristicAssociateRuleNum', '')
                })

        # 返回响应数据
        return jsonify({"data": {"transactions": response_data}, "code": 20000})
    except Exception as e:
        app.logger.error(f'Error processing request: {e}')
        return jsonify({'error': str(e)}), 500


# @app.route('/GetHeuristicRulesⅠ_IndirectTransfer', methods=['GET'])
# def GetHeuristicRulesⅠ_IndirectTransfer():
#     address = request.args.get('address')
#     if not address:
#         app.logger.error('No address provided')
#         return jsonify([]), 400
#
#     try:
#         # 创建 MongoDB 查询
#         query = {'$or': [{'from': address}, {'to': address}]}
#         cursor = DB_IndirectTransfer.find(query)
#         # 将查询结果转换为列表
#         transactions_list = list(cursor)
#
#         # 如果没有找到相关的交易，返回404
#         if not transactions_list:
#             return jsonify({'message': 'Sorry, No associated transactions found!/(ㄒoㄒ)/~~'})
#
#         # 准备响应数据
#         response_data = []
#         for transaction in transactions_list:
#             # 移除 MongoDB 自动生成的 _id 字段
#             transaction.pop('_id', None)
#
#             # 检查 address 是否同时出现在 from 和 to 字段中
#             if address == transaction['from'] and address == transaction['to']:
#                 # 地址同时出现在 from 和 to，添加双向关联账户
#                 associated_accounts = [transaction['from'], transaction['to']]
#             elif address == transaction['from']:
#                 # 地址只出现在 from，添加 to 作为关联账户
#                 associated_accounts = [transaction['to']]
#             else:
#                 # 地址只出现在 to，添加 from 作为关联账户
#                 associated_accounts = [transaction['from']]
#
#             # 构建每个交易的响应数据
#             for acc in associated_accounts:
#                 # Format 'hash'
#                 transaction['hash'] = format_hash(transaction['hash'])
#                 response_data.append({
#                     'associated_account': acc,
#                     'asset_pool': transaction['asset_pool'],
#                     'hash': transaction['hash'],
#                     'HeuristicAssociateRuleNum': transaction['HeuristicAssociateRuleNum']
#                 })
#
#         # 返回响应数据
#         return jsonify({"data": {"transactions": response_data}, "code": 20000})
#     except Exception as e:
#         app.logger.error(f'Error processing request: {e}')
#         return jsonify({'error': str(e)}), 500


@app.route('/GetHeuristicRulesⅡ_CrossChainTransfer', methods=['GET'])
def GetHeuristicRulesⅡ_CrossChainTransfer():
    address = request.args.get('address')
    if not address:
        app.logger.error('No address provided')
        return jsonify([]), 400

    try:
        df = pd.read_csv('DataDB/new_linked_cross_chain_transactions.csv')
        response_data = find_associated_crosschaintx(address, df)

        if not response_data:
            return jsonify({'message': 'Sorry, No associated transactions found!/(ㄒoㄒ)/~~'})

        return jsonify({"data": {"transactions": response_data}, "code": 20000})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/GetHeuristicRulesⅢ_ERC20TransferLink', methods=['GET'])
def GetHeuristicRulesⅢ_ERC20TransferLink():
    address = request.args.get('address')
    if not address:
        app.logger.error('No address provided')
        return jsonify([]), 400

    try:
        df = pd.read_csv('DataDB/new_TC_ERC20Transfer_Link.csv')
        response_data = find_associated_ERC20TX(address, df)

        if not response_data:
            return jsonify({'message': 'Sorry, No associated transactions found!/(ㄒoㄒ)/~~'})

        return jsonify({"data": {"transactions": response_data}, "code": 20000})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/data_compute', methods=['GET'])
def data_compute():
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 100, type=int)
    address = request.args.get('address')

    if not address:
        app.logger.error('No address provided')
        return jsonify([]), 400

    query = {}

    if address:
        query = {'$or': [{'from': address}, {'to': address}]}

    paginated_data = DB_data_compute.find(query).skip((page - 1) * per_page).limit(per_page)

    transactions = list(paginated_data)
    for transaction in transactions:
        transaction.pop('_id', None)  # 移除没用的字段MongoDB _id

        timestamp_utc = datetime.fromtimestamp(transaction['timeStamp'], tz=timezone.utc)
        timestamp_beijing = timestamp_utc + timedelta(hours=8)
        transaction['timeStamp'] = timestamp_beijing.strftime('%Y-%m-%d %H:%M:%S')

        transaction['value'] = int(transaction['value']) / 10 ** 18

    deposit_count = DB_data_compute.count_documents({'flag': 1})
    withdrawal_count = DB_data_compute.count_documents({'flag': -1})

    transactions = {
        "transactions": transactions,
        "deposit_count": deposit_count,
        "withdrawal_count": withdrawal_count,
        "total": DB_data_compute.count_documents(query),
        "page": page,
        "per_page": per_page
    }

    return jsonify({"data": transactions, "code": 20000})


"""
@app.route('/data_compute', methods=['GET'])
def data_compute():
    address = request.args.get('address')
    df = pd.read_csv('data/TornadoCash_Info/eth_01.csv')
    # directory = 'data/TornadoCash_Info/'

    # all_files = [os.path.join(directory, file) for file in os.listdir(directory) if file.endswith('.csv')]
    # df = pd.concat((pd.read_csv(f) for f in all_files), ignore_index=True)

    if address:
        df = df[(df['from'] == address) | (df['to'] == address)]

    df['timeStamp'] = pd.to_datetime(df['timeStamp'], unit='s', utc=True)
    df['timeStamp'] = df['timeStamp'].dt.tz_convert('Asia/Shanghai').dt.strftime('%Y-%m-%d %H:%M:%S')

    df['value'] = df['value'] / 10 ** 18

    deposit_count = df[df['flag'] == 1].shape[0]
    withdrawal_count = df[df['flag'] == -1].shape[0]
#
    transactions = df[['timeStamp', 'value']].to_dict(orient='records')

    # Add the statistics to the response
    transactions = {
        "data": transactions,
        "deposit_count": deposit_count,
        "withdrawal_count": withdrawal_count,
        "code": 20000
    }

    return jsonify({"data": {"transactions": transactions}, "code": 20000})
"""


@app.route('/get_transactions', methods=['GET'])
def get_transactions():
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 100, type=int)
    address = request.args.get('address')

    query = {}
    if address:
        query = {'$or': [{'from': address}, {'to': address}]}

    paginated_data = DB_get_transactions.find(query).skip((page - 1) * per_page).limit(per_page)

    transactions = list(paginated_data)
    for transaction in transactions:
        # Remove the MongoDB _id
        transaction.pop('_id', None)

        timestamp_utc = datetime.fromtimestamp(transaction['timeStamp'], tz=timezone.utc)
        timestamp_beijing = timestamp_utc + timedelta(hours=8)
        transaction['timeStamp'] = timestamp_beijing.strftime('%Y-%m-%d %H:%M:%S')
        transaction['value'] = int(transaction['value']) / 10 ** 18

        # Map 'flag' to '存款' or '提款'
        transaction['flag'] = {1: '存款', -1: '提款'}.get(transaction['flag'], transaction['flag'])

        # Format 'hash'
        transaction['hash'] = format_hash(transaction['hash'])

    total_count = DB_get_transactions.count_documents(query)
    transactions = {
        "transactions": transactions,
        "total": total_count,
        "page": page,
        "per_page": per_page
    }

    return jsonify({"data": transactions, "code": 20000})


@app.route('/token_search', methods=['GET'])
def token_search():
    address = request.args.get('address')
    if not address:
        app.logger.error('No address provided')
        return jsonify([]), 400

    try:
        query = {'$or': [{'deposit': address}, {'withdraw': address}]}
        cursor = DB_search.find(query)
        df = pd.DataFrame(list(cursor))
        pattern = re.compile(r'"([^"]*)"')
        # print(df.columns)
        df['link_list'] = df['link_list'].apply(lambda x: pattern.search(x).group(1) if pattern.search(x) else x)
        # 设置标志位
        df['flag'] = df.apply(lambda x: 1 if x['deposit'] == address else 0 if x['withdraw'] == address else -1, axis=1)
        # 过滤匹配的行
        matched_rows = df[df['flag'] != -1]

        account_dict = {}

        for _, row in matched_rows.iterrows():
            # 输入的地址刚好也某个交易的'deposit'列也在某个交易的'withdraw'列出现，那么写一个for循环再遍历一遍对地址=='deposit'列，则添加'withdraw'列地址进去account_list；同时再地址=='withdraw'列，则添加'deposit'列地址进去account_list
            if row['deposit'] == address and row['withdraw'] == address:
                # 遍历数据集，根据flag的值添加对应列的地址到account_list
                for _, inner_row in df.iterrows():
                    if inner_row['flag'] == 1:
                        account_dict[inner_row['withdraw']] = (inner_row['link_list'], inner_row['asset_pool'], inner_row['HeuristicAssociateRuleNum'])

                    elif inner_row['flag'] == 0:
                        account_dict[inner_row['deposit']] = (inner_row['link_list'], inner_row['asset_pool'], inner_row['HeuristicAssociateRuleNum'])
            # 对输入地址只单独出现在'deposit'列或者'withdraw'列的地址，则保存其对应的'withdraw'列地址或者'deposit'列地址到account_list
            else:
                # 单独出现在一列中
                associated_account = row['withdraw'] if row['deposit'] == address else row['deposit']
                account_dict[associated_account] = (row['link_list'], row['asset_pool'], row['HeuristicAssociateRuleNum'])

        response_data = [{
            'associated_account': account,
            'HeuristicAssociateRuleNum': rule_num,
            'link_list': link_list,
            'asset_pool': asset_pool
        } for account, (link_list, asset_pool, rule_num) in account_dict.items()]

        if not account_dict.items():
            app.logger.info('No matching address found for the address_link')
            return jsonify([])

        if not response_data:
            app.logger.info('No matching data found for the address')
            return jsonify([])

        return jsonify({"data": {"transactions": response_data}, "code": 20000})
    except Exception as e:
        app.logger.error(f'Error processing request: {e}')
        return jsonify({"error": "没有该请求地址的混币关联账户信息: " + str(e)}), 500


@app.route('/dapp_search', methods=['GET'])
def dapp_search():
    address = request.args.get('address')
    if not address:
        app.logger.error('No address provided')
        return jsonify([]), 400

    try:
        query = {'$or': [{'deposit': address}, {'withdraw': address}]}
        cursor = DB_Dapp.find(query)
        df = pd.DataFrame(list(cursor))
        pattern = re.compile(r'"([^"]*)"')
        # print(df.columns)
        df['dapp_list'] = df['dapp_list'].apply(lambda x: pattern.search(x).group(1) if pattern.search(x) else x)
        # 设置标志位
        df['flag'] = df.apply(lambda x: 1 if x['deposit'] == address else 0 if x['withdraw'] == address else -1, axis=1)
        # 过滤匹配的行
        matched_rows = df[df['flag'] != -1]

        account_dict = {}

        for _, row in matched_rows.iterrows():
            # 输入的地址刚好也某个交易的'deposit'列也在某个交易的'withdraw'列出现，那么写一个for循环再遍历一遍对地址=='deposit'列，则添加'withdraw'列地址进去account_list；同时再地址=='withdraw'列，则添加'deposit'列地址进去account_list
            if row['deposit'] == address and row['withdraw'] == address:
                # 遍历数据集，根据flag的值添加对应列的地址到account_list
                for _, inner_row in df.iterrows():
                    if inner_row['flag'] == 1:
                        account_dict[inner_row['withdraw']] = (
                        inner_row['dapp_list'], inner_row['HeuristicAssociateRuleNum'], row['dapp_count'])

                    elif inner_row['flag'] == 0:
                        account_dict[inner_row['deposit']] = (
                        inner_row['dapp_list'], inner_row['HeuristicAssociateRuleNum'], row['dapp_count'])
            # 对输入地址只单独出现在'deposit'列或者'withdraw'列的地址，则保存其对应的'withdraw'列地址或者'deposit'列地址到account_list
            else:
                # 单独出现在一列中
                associated_account = row['withdraw'] if row['deposit'] == address else row['deposit']
                account_dict[associated_account] = (
                row['dapp_list'], row['HeuristicAssociateRuleNum'], row['dapp_count'])

        response_data = [{
            'associated_account': account,
            'HeuristicAssociateRuleNum': rule_num,
            'dapp_list': dapp_list,
            'dapp_count': dapp_count,
        } for account, (dapp_list, rule_num, dapp_count) in account_dict.items()]

        if not account_dict.items():
            app.logger.info('No matching address found for the address_link')
            return jsonify([])

        if not response_data:
            app.logger.info('No matching data found for the address')
            return jsonify([])

        return jsonify({"data": {"transactions": response_data}, "code": 20000})
    except Exception as e:
        app.logger.error(f'Error processing request: {e}')
        return jsonify({"error": "没有该请求地址的混币关联账户信息: " + str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True)

