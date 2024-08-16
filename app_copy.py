import csv
import pandas as pd
import re
from flask import Flask, jsonify, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app, supports_credentials=True)


@app.route('/')
def hello_world():
    return 'Hello World!'


@app.route('/get_transactions', methods=['GET'])
def get_transactions():
    address = request.args.get('address')

    df = pd.read_csv('data/query/eth_01.csv')
    if address:
        df = df[(df['from'] == address) | (df['to'] == address)]

    df['timeStamp'] = pd.to_datetime(df['timeStamp'], unit='s', utc=True)
    df['timeStamp'] = df['timeStamp'].dt.tz_convert('Asia/Shanghai').dt.strftime('%Y-%m-%d %H:%M:%S')

    df['value'] = df['value'] / 10 ** 18
    df['flag'] = df['flag'].map({1: '存款', -1: '提款'})
    transactions = df[['hash', 'timeStamp', 'value', 'flag']].to_dict(orient='records')

    return jsonify({"data": {"transactions": transactions}, "code": 20000})


@app.route('/search', methods=['GET'])
def search():
    address = request.args.get('address')
    if not address:
        app.logger.error('No address provided')
        return jsonify([]), 400

    try:
        df = pd.read_csv('data/AccountLink_Query/test.csv')
        pattern = re.compile(r'"([^"]*)"')
        df['link_list'] = df['link_list'].apply(lambda x: pattern.search(x).group(1) if pattern.search(x) else x)
        # 设置标志位
        df['flag'] = df.apply(lambda x: 1 if x['deposit'] == address else 0 if x['withdraw'] == address else -1, axis=1)
        # 过滤匹配的行
        matched_rows = df[df['flag'] != -1]
        # matched_rows = df[(df['deposit'] == address) | (df['withdraw'] == address)]

        account_list = []

        for _, row in matched_rows.iterrows():
            # 输入的地址刚好也某个交易的'deposit'列也在某个交易的'withdraw'列出现，那么写一个for循环再遍历一遍对地址=='deposit'列，则添加'withdraw'列地址进去account_list；同时再地址=='withdraw'列，则添加'deposit'列地址进去account_list
            if row['deposit'] == address and row['withdraw'] == address:
                # 遍历数据集，根据flag的值添加对应列的地址到account_list
                for _, inner_row in df.iterrows():
                    if inner_row['flag'] == 1:
                        account_list.append(inner_row['withdraw'])

                    elif inner_row['flag'] == 0:
                        account_list.append(inner_row['deposit'])

            # 对输入地址只单独出现在'deposit'列或者'withdraw'列的地址，则保存其对应的'withdraw'列地址或者'deposit'列地址到account_list
            else:
                # 单独出现在一列中
                if row['deposit'] == address:
                    account_list.append(row['withdraw'])
                elif row['withdraw'] == address:
                    account_list.append(row['deposit'])
        # 去除重复项
        account_list = list(set(account_list))
        response_data = [{
            'associated_account': account,
            'HeuristicAssociateRuleNum': row['HeuristicAssociateRuleNum'],
            'link_list': row['link_list']
        } for account in account_list]

        if not account_list:
            app.logger.info('No matching address found for the address_link')
            return jsonify([])

        if not response_data:
            app.logger.info('No matching data found for the address')
            return jsonify([])

        return jsonify({"data": {"transactions": response_data}, "code": 20000})
    except Exception as e:
        app.logger.error(f'Error processing request: {e}')
        return jsonify({"error": "An error occurred while processing your request: " + str(e)}), 500

        # # Remove duplicates and create response data
        # account_list = list(set(account_list))
        #
        # for account in account_list:
        #     matching_rows = df[(df['deposit'] == account) | (df['withdraw'] == account)]
        #     for _, match_row in matching_rows.iterrows():
        #         response_data.append({
        #             'associated_account': account,
        #             'HeuristicAssociateRuleNum': match_row['HeuristicAssociateRuleNum'],
        #             'link_list': match_row['link_list']
        #         })