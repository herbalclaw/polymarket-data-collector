import asyncio
import websockets
import json
import time
import requests

CLOB_WS_URL = 'wss://ws-subscriptions-clob.polymarket.com/ws/market'

now = int(time.time())
window = (now // 300) * 300
slug = f'btc-updown-5m-{window}'
resp = requests.get('https://gamma-api.polymarket.com/events', params={'slug': slug}, timeout=10)
data = resp.json()
market = data[0]['markets'][0]
token_ids = json.loads(market.get('clobTokenIds', '[]'))
print(f'Market: {slug}')

async def test():
    async with websockets.connect(CLOB_WS_URL) as ws:
        msg = {
            'action': 'subscribe',
            'subscriptions': [
                {'topic': 'market', 'type': 'book', 'market': token_ids[0]},
                {'topic': 'market', 'type': 'book', 'market': token_ids[1]}
            ]
        }
        await ws.send(json.dumps(msg))
        print('Subscribed, waiting...')
        
        for i in range(10):
            try:
                data = await asyncio.wait_for(ws.recv(), timeout=3.0)
                parsed = json.loads(data)
                et = parsed.get('event_type')
                print(f'{i}: {et}')
            except asyncio.TimeoutError:
                print(f'{i}: timeout')

asyncio.run(test())
