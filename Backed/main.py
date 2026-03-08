
import asyncio
import websockets
import json
import logging
from routing_core import handle_navigation, init_globals

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

async def main():
    logger.info("Starting WebSocket server on ws://localhost:5000")
    init_globals()
    async with websockets.serve(handle_navigation, "localhost", 5000):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
