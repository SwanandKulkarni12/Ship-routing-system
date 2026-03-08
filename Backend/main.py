import asyncio
import json
import logging
import os
import base64
from aiohttp import web
from aiohttp.web import middleware
from routing_core import handle_navigation, init_globals

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

@middleware
async def cors_middleware(request, handler):
    if request.method == 'OPTIONS':
        resp = web.Response()
    else:
        resp = await handler(request)
    resp.headers['Access-Control-Allow-Origin'] = '*'
    resp.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
    resp.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    return resp

async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    await handle_navigation(ws)
    return ws

async def upload_screenshot(request):
    """Receives a base64-encoded PNG from the frontend and saves it."""
    try:
        data = await request.json()
        img_data = data.get('image', '')
        # Strip data URL prefix if present
        if ',' in img_data:
            img_data = img_data.split(',')[1]
        
        img_bytes = base64.b64decode(img_data)
        save_path = os.path.join(os.path.dirname(__file__), 'route_visualization.png')
        with open(save_path, 'wb') as f:
            f.write(img_bytes)
        
        logger.info(f"Map screenshot saved: {save_path} ({len(img_bytes)} bytes)")
        return web.json_response({'status': 'ok', 'size': len(img_bytes)})
    except Exception as e:
        logger.error(f"Screenshot upload failed: {e}")
        return web.json_response({'status': 'error', 'message': str(e)}, status=500)

async def main():
    logger.info("Starting aiohttp server on http://localhost:5000")
    init_globals()
    
    app = web.Application(middlewares=[cors_middleware])
    app.router.add_get('/', websocket_handler)
    app.router.add_post('/upload-screenshot', upload_screenshot)
    
    # Serve static files (PDF Reports)
    report_name = os.getenv('AI_REPORT_NAME', 'voyage_report.pdf')
    report_path = os.path.join(os.path.dirname(__file__), report_name)
    app.router.add_static('/reports/', os.path.dirname(__file__))
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, 'localhost', 5000)
    await site.start()
    
    print(f"Server ready at ws://localhost:5000 and http://localhost:5000/reports/{report_name}")
    await asyncio.Future()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass