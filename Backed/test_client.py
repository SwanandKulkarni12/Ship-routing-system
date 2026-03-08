import asyncio
import json
import websockets
import time
TEST_CASES = [{'name': 'Short Distance - Baltic Sea', 'payload': {'start': [18.5, 54.5], 'end': [24.5, 59.5], 'mode': 'distance'}}, {'name': 'Medium Distance - North Atlantic (Balanced)', 'payload': {'start': [-60.0, 45.0], 'end': [-10.0, 50.0], 'mode': 'balanced'}}, {'name': 'Long Distance - Trans-Pacific (Safety)', 'payload': {'start': [140.0, 35.0], 'end': [-125.0, 45.0], 'mode': 'safety'}}, {'name': 'Medium Distance - Indian Ocean (Fuel)', 'payload': {'start': [55.0, 20.0], 'end': [100.0, -5.0], 'mode': 'fuel consumption'}}]

async def run_test(case):
    uri = 'ws://localhost:5000'
    print(f"\n--- Running Test: {case['name']} (Mode: {case['payload']['mode']}) ---")
    try:
        async with websockets.connect(uri, max_size=None, ping_interval=None, ping_timeout=None) as websocket:
            start_time = time.time()
            await websocket.send(json.dumps(case['payload']))
            while True:
                response = await websocket.recv()
                data = json.loads(response)
                if data['type'] == 'progress':
                    print(f"[{data['pct']}%] {data.get('step', '')}")
                elif data['type'] == 'final':
                    metrics = data.get('metrics', {})
                    opt = metrics.get('optimized', {})
                    astar = metrics.get('astar', {})
                    print('\n[SUCCESS] Final payload received.')
                    print(f'Time Taken: {time.time() - start_time:.2f} seconds')
                    print(f"Distance: Opt {opt.get('distance_km')} km vs A* {astar.get('distance_km')} km")
                    if opt.get('eta_hours') is not None:
                        print(f"ETA: Opt {opt.get('eta_hours')} hrs vs A* {astar.get('eta_hours')} hrs")
                    if opt.get('fuel_tonnes') is not None:
                        print(f"Fuel: Opt {opt.get('fuel_tonnes')} tonnes vs A* {astar.get('fuel_tonnes')} tonnes")
                    if data.get('alternatives'):
                        print(f"Alternatives provided: {len(data['alternatives'])}")
                    break
                elif data['type'] == 'error':
                    print(f"\n[ERROR] Server returned error: {data.get('message')}")
                    break
    except Exception as e:
        print(f'[TEST EXCEPTION] {e}')

async def main():
    print('Starting WebSocket tests based on different modes and coordinates...')
    for case in TEST_CASES:
        await run_test(case)
        await asyncio.sleep(1)
if __name__ == '__main__':
    asyncio.run(main())