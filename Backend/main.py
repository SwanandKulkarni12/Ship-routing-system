import asyncio
import json
import logging
import os
from aiohttp import web
from routing_core import handle_navigation, init_globals

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    await handle_navigation(ws)
    return ws

async def main():
    logger.info("Starting aiohttp server on http://localhost:5000")
    init_globals()
    
    app = web.Application()
    app.router.add_get('/', websocket_handler)
    
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