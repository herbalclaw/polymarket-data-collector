#!/usr/bin/env python3
"""Test WebSocket connection to Polymarket."""

import asyncio
import websockets
import json
import time

async def test_websocket():
    uri = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    
    print("Connecting to WebSocket...")
    async with websockets.connect(uri, ping_interval=20, ping_timeout=10) as ws:
        print("✅ Connected!")
        
        # Try different subscription formats
        subscriptions = [
            # Format 1: Using token ID (asset_id)
            {
                "type": "subscribe",
                "channel": "market",
                "market": "87462146635212028418657665778854181324848565361077467813174779850930903654433"
            },
            # Format 2: Using condition ID
            # {
            #     "type": "subscribe",
            #     "channel": "market",
            #     "market": "0x..."
            # },
        ]
        
        for sub in subscriptions:
            print(f"\nSubscribing: {sub}")
            await ws.send(json.dumps(sub))
            
            # Listen for 10 seconds
            start = time.time()
            while time.time() - start < 10:
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=1)
                    data = json.loads(msg)
                    print(f"📨 Message: {json.dumps(data, indent=2)[:500]}")
                except asyncio.TimeoutError:
                    print("⏱️  No message (1s timeout)")
                    continue
                except Exception as e:
                    print(f"❌ Error: {e}")
                    break

if __name__ == "__main__":
    asyncio.run(test_websocket())
