from tenacity import retry, stop_after_attempt, wait_fixed
import requests
from datetime import datetime
from decimal import Decimal
from sqlalchemy.sql import text


@retry(wait=wait_fixed(5))
def get_symbol_decimals_dict():
    data = requests.get("https://api.zksync.io/api/v0.1/tokens").json()
    return {x["symbol"]: x["decimals"] for x in data}


@retry(wait=wait_fixed(5))
def get_l1_address_txs(address, api_key):
    txs = []

    # normal
    data = requests.get(
        f"https://api.etherscan.io/api?module=account&action=txlist&address={address}&apikey={api_key}").json()
    if data['status'] == '1':
        txs.extend(data['result'])

    # erc-20
    data = requests.get(
        f"https://api.etherscan.io/api?module=account&action=tokentx&address={address}&apikey={api_key}").json()
    if data['status'] == '1':
        txs.extend(data['result'])

    return txs


@retry(wait=wait_fixed(5))
def get_zksync_address_txs(address):
    offset = 0
    limit = 100

    txs = []
    while offset == 0 or data:
        res = requests.get(f"https://api.zksync.io/api/v0.1/account/{address}/history/{offset}/{limit}")
        if res.status_code == 404:
            break

        data = res.json()
        txs.extend(data)

        offset += limit

    return txs


def insert_l1_txs(txs, engine, tx_table):
    if not txs:
        return

    engine.execute(
        tx_table.insert(),
        [
            {
                'layer': 'l1', 'tx': tx['hash'],
                'from': tx['from'], 'to':tx['to'],
                'symbol':tx.get('tokenSymbol', 'ETH')[:40],
                'amount': min(Decimal(tx['value']) / 10**int(tx.get('tokenDecimal', '18')), Decimal(10**20)),
                'created_on':datetime.utcfromtimestamp(int(tx['timeStamp']))
            }
            for tx in txs
        ]
    )


def insert_zksync_txs(txs, engine, tx_table, symbol_decimals_dict):
    if not txs:
        return

    engine.execute(
        tx_table.insert(),
        [
            {
                'layer': 'zksync', 'tx': '0x' + tx['hash'].split(':')[1],
                'from': tx['tx']['from'], 'to':tx['tx']['to'],
                'symbol':tx['tx']['token'],
                'amount':Decimal(tx['tx']['amount']) / 10**symbol_decimals_dict[tx['tx']['token']],
                'created_on':datetime.strptime(tx['created_at'][:19], "%Y-%m-%dT%H:%M:%S")
            }
            for tx in txs
        ]
    )


def tx_count_between_accounts(address_1, address_2, engine):
    count = engine.execute(text("""
        SELECT COUNT(*) FROM (
            SELECT
                * 
            FROM `tx`
            WHERE 
                `to` IN (SELECT `address` FROM `account_address` WHERE `account`= :address_1) AND
                `from` IN (SELECT `address` FROM `account_address` WHERE `account`= :address_2)
            UNION
            SELECT
                * 
            FROM `tx`
            WHERE 
                `to` IN (SELECT `address` FROM `account_address` WHERE `account`= :address_2) AND
                `from` IN (SELECT `address` FROM `account_address` WHERE `account`= :address_1)
        ) AS `t1`
    """), address_1=address_1, address_2=address_2).fetchone()[0]

    return count


def tx_count_between_grants(grant_1, grant_2, engine):
    count = engine.execute(text("""
        SELECT COUNT(*) FROM (
            SELECT
                * 
            FROM `tx`
            WHERE 
                `to` IN (SELECT `address` FROM `grant` WHERE `id`= :grant_1) AND
                `from` IN (SELECT `address` FROM `grant` WHERE `id`= :grant_2)
            UNION
            SELECT
                * 
            FROM `tx`
            WHERE 
                `to` IN (SELECT `address` FROM `grant` WHERE `id`= :grant_2) AND
                `from` IN (SELECT `address` FROM `grant` WHERE `id`= :grant_1)
        ) AS `t1`
    """), grant_1=grant_1, grant_2=grant_2).fetchone()[0]

    return count
