import websockets.asyncio.client as clientModule
from websockets.asyncio.server import serve
# import websockets.asyncio.client.ClientConnection
import asyncio
# time, os
# redis
import orjson, json
# import pyqtgraph as pg
# import sys
# from PyQt6.QtWidgets import QApplication
# from qasync import QEventLoop, asyncSlot

# if QApplication.instance() is None:
#     app = QApplication(sys.argv)

# loop = QEventLoop(app)

## #TODO: clean up redis code
# redis_ctx = redis.from_url(os.environ['REDIS_URL.env'])
orderBookSubTrigger = asyncio.Event()
notifications_queue = asyncio.Queue()
OrderBook = {
    "bids": {},
    "asks": {}
}

## #NTIU: creates a websocket connection context to deribit
async def wsConnection():
    async with clientModule.connect("wss://test.deribit.com/ws/api/v2") as ws:
        print ("[LOG]\tConnection Live.")


async def subRequest(ws: clientModule.ClientConnection, req):
    await ws.send (json.dumps(req))

## websocket server
async def fe_serve():
    async with serve(requestHandler, "localhost", 8765) as server:
        await server.serve_forever()

## #TODO: update the in memory orderbook on very notification
##          and send the updated orderbook to frontend
async def fe_sub(websocket):
    while True:
        await orderBookSubTrigger.wait()
        badata = await notifications_queue.get()
        # print ("badata", badata)
        await websocket.send(orjson.dumps(badata))
        print ("fe data sent")
        orderBookSubTrigger.clear() if notifications_queue.empty() else None

async def requestHandler(websocket):
    ## #TODO: enfore json format and implement a standard [prototbuf, maybe in future]
    async for msg in websocket:
        msg = orjson.loads(msg)
        print ("pasred fe msg", msg)
        match msg["requestType"]:
            case "subOB":
                print ("[LOG]\tFrontend Subscribed to OB.")
                asyncio.create_task(fe_sub(websocket))

## #TODO: Implement a better logging system
async def wsListener(ws: clientModule.ClientConnection, queue: asyncio.Queue):
    print ("[LOG]\tListener Live.")
    async for snap in ws:
        data = orjson.loads(snap)

        # for a in data["params"]["data"].keys():

        # redis_ctx.set(f"{time.time()}", data["params"]["data"])
        print ("-----------------------------------")
        # print (snap)
        for a in data.keys():
            if a == "result" and data[a] == "book.BTC-PERPETUAL.100ms":
                print("[LOG]\tACK Recieved.")

            ## #TODO:   since the data is aggregated by price, maintain a dict
            ##          object by price, so as the changes can be made easily
            ## #DONE:   need to be tested with frontend
            if a == "params":
                bids=[]
                asks=[]
                minPrice = data[a]["data"]["bids"][0][1] if len(data[a]["data"]["bids"]) > 0 else float('inf')
                # for b in data[a]["data"]["bids"]:
                #     if b[1] < minPrice:
                #         minPrice = b[1]
                # for b in data[a]["data"]["asks"]:
                #     asks.append([b[1],b[2]])
                    
                # queue.put_nowait({ "startPrice": minPrice, "bids": bids, "asks": asks})
                if data[a]["data"]["type"] == "snapshot":
                    for mbp in data[a]["data"]["bids"]:
                        if mbp[1] < minPrice:
                            minPrice = mbp[1]
                        OrderBook["bids"][f"{mbp[1]}"] = mbp[2]
                    for mbp in data[a]["data"]["asks"]:
                        OrderBook["asks"][f"{mbp[1]}"] = mbp[2]
                    # OrderBook["minPrice"] = minPrice
                    print("[LOG]\tFirst Snap Recieved.")
                    orderBookSubTrigger.set()
                if data[a]["data"]["type"] == "change":
                    for mbp in data[a]["data"]["bids"]:
                        if mbp[1] < minPrice:
                            minPrice = mbp[1]
                        match mbp[0]:
                            case 'new' | 'change':
                                OrderBook["bids"][f"{mbp[1]}"] = mbp[2]
                            case 'delete':
                                del OrderBook["bids"][f"{mbp[1]}"]
                    for mbp in data[a]["data"]["asks"]:
                        match mbp[0]:
                            case 'new' | 'change':
                                OrderBook["asks"][f"{mbp[1]}"] = mbp[2]
                            case 'delete':
                                del OrderBook["asks"][f"{mbp[1]}"]
                    # OrderBook["minPrice"] = minPrice
                    notifications_queue.task_done() if not notifications_queue.empty() else None
                    tmpDict = {
                        "bids": sorted(list( OrderBook["bids"].items()), key=lambda x: float(x[0])),
                        "asks": sorted(list( OrderBook["asks"].items()), key=lambda x: float(x[0])),
                        "minPrice": minPrice
                    }
                    # prv = 0
                    # for k in tmpDict["asks"]:
                    #     k[1] = k[1] + prv
                    #     prv = k[1]
                    # i = len(tmpDict["bids"]) - 1
                    # prv = 0
                    # while (i > 0):
                    #     tmpDict["bids"][i][1] = tmpDict["bids"][i][1] + prv
                    #     prv = tmpDict["bids"][i][1]
                    notifications_queue.put_nowait(tmpDict)
                    orderBookSubTrigger.set()
                    print("[LOG]\tUpdate Notification Recieved.")


async def obviz():
    subReq = {
        "method": "public/subscribe",
        "params": {
            "channels": [
                "book.BTC-PERPETUAL.agg2"
            ]
        },
        "jsonrpc": "2.0",
        "id": 2
    }

    ## create connection to deribit
    async with clientModule.connect("wss://www.deribit.com/ws/api/v2") as ws_ctx:
        print ("[LOG]\tConnection Live.")
        print(f"[LOG]\t{ws_ctx.latency} latency.")
        
        asyncio.create_task(wsListener(ws_ctx, notifications_queue))
        ## websocket for frontend
        asyncio.create_task(fe_serve())
        
        ## sub request to beribit for L2 data
        await subRequest(ws_ctx, subReq)
        await ws_ctx.keepalive()


# Run asyncio and Qt event loop together
# asyncio.set_event_loop(loop)
# try:
#     with loop:
#         loop.run_until_complete(obviz())
# except KeyboardInterrupt:
#     print("\n[LOG]\tInterrupt received. Shutting down...")
#     loop.stop()

asyncio.run(obviz())