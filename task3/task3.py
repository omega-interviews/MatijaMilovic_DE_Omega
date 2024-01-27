import pandas as pd
from flask import Flask, jsonify


app = Flask(__name__)


@app.route('/', methods=['GET'])
def totalAmountSeriesA():
    csv_path = 'python_task_data.csv'
    df = pd.read_csv(csv_path, usecols=['raisedAmt', 'round'], dtype={
        'raisedAmt': 'int32', 'round': 'category'})

    df = df[df['round'] == 'a']

    total_amount = int(df['raisedAmt'].sum())
    average_amount = total_amount / len(df['raisedAmt'])

    print(f'Total Amount for Series A: ${total_amount:,}')
    print(f'Average Amount for Series A: ${average_amount:,.2f}')
    res = {'total_amount': total_amount}
    return jsonify(res)


if __name__ == '__main__':

    app.run(debug=True, port=5000, host='0.0.0.0')
