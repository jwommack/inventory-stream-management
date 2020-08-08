import faust
from deprecated import deprecated
from pymongo import MongoClient
import asyncio
import json
import functools
import copy
import logging
from typing import List
from random import randrange 
from uuid import UUID

logger = logging.getLogger()
logger.setLevel(logging.ERROR)

app = faust.App(
    'fausted',
    broker='kafka://localhost:9092',
    store='memory://',
    processing_guarantee='exactly_once',
    topic_partitions=1
)
# connect = MongoClient('localhost:27017')
# db = connect.inventory
# collection = db.inventoryCollection
OUTPUT_LOG = ''
CURRENT_INVENTORY = {
    'A': 150,
    'B': 150,
    'C': 100,
    'D': 100,
    'E': 200,
}
STARTING_INVENTORY = {
    'A': 150,
    'B': 150,
    'C': 100,
    'D': 100,
    'E': 200,
}

class InventoryModel(faust.Record):
    product: str
    on_hand: int

class OrderLineModel(faust.Record):
    Product: str
    Quantity: int

class OrderModel(faust.Record):
    Header: int
    Lines: List[OrderLineModel]

def print_log(value):
    print(OUTPUT_LOG)
    logger.debug(CURRENT_INVENTORY)

def inventory_sink(value):
    CURRENT_INVENTORY = value

def build_output(value):
    global OUTPUT_LOG
    OUTPUT_LOG = '{}\n{}'.format(OUTPUT_LOG, value)

inventory_table = app.GlobalTable('inventory', default=int, key_type=bytes,
                            value_type=InventoryModel, partitions=1)

stock = app.topic('stock', value_type=InventoryModel, value_serializer='json')
ordering = app.topic('ordering', value_type=OrderModel, value_serializer='json')
fulfillment_log = app.topic('fulfillment_log')
inventory = app.topic('inventory')
seed = app.topic('seed_orders')

@app.agent(stock, sink=[inventory_sink])
async def update_inventory(stream):
    async for event in stream:
        inventory_table[event.product] = event.on_hand
        logger.info('Resetting inventory for {} to {} increased by {}'.format(
            event.product, inventory_table[event.product], event.on_hand))
        # Ideal sink is one, here due to stream behavior, limited calls, and time
        yield inventory_table.values()

@app.agent(ordering, sink=[inventory_sink])
async def process_order(stream):
    async for event in stream:
        requested = { 'A': 0, 'B': 0, 'C': 0, 'D': 0, 'E': 0 }
        fulfilled = { 'A': 0, 'B': 0, 'C': 0, 'D': 0, 'E': 0 }
        backorder = { 'A': 0, 'B': 0, 'C': 0, 'D': 0, 'E': 0 }
        logger.debug('Inventory Remains: ',
                     any( x > 0 for x in CURRENT_INVENTORY.values()))
        # Extra break to catch fallthrough
        if (all( x <= 0 for x in CURRENT_INVENTORY.values())):
            return
        
        for line in event.Lines:
            requested[line.Product] += line.Quantity
            # Initial quantity of inventory
            start_qty = copy.copy(inventory_table[line.Product])
            # requested reduction
            inventory_table[line.Product] -= line.Quantity
            # Update current inventory total
            CURRENT_INVENTORY[line.Product] = inventory_table[line.Product]
            # end inventory
            end_qty = copy.copy(inventory_table[line.Product])
            # Prefer branchless for performance but readability++ here.
            logger.debug('({}: {}) {} {}  {}'.format(line.Product, 
                inventory_table[line.Product], start_qty, end_qty, line.Quantity))
            if(start_qty > 0):
                # Positive Inventory
                if (end_qty > 0):
                    # positive remaining Inventory is as straight fulfill
                    fulfilled[line.Product] += line.Quantity
                else:
                    # Partial back order partial fulfill
                    fulfilled[line.Product] += line.Quantity - abs(end_qty)
                    backorder[line.Product] += abs(end_qty)
            else:
                # Negative starting inventory is a straight backorder
                backorder[line.Product] += line.Quantity
        await asyncio.sleep(0.1)
        await fulfillment_log.send(value="{}: {} :: {} :: {}".format(
            event.Header,
            tuple(requested.values()),
            tuple(fulfilled.values()),
            tuple(backorder.values())
        ))
        yield inventory_table.values()

@app.agent(fulfillment_log, sink=[build_output])
async def update_fulfillment_log(stream):
    # Format output
    async for event in stream:
        log = event.replace('(','').replace(')','')
        yield log

@app.agent(seed, sink=[print_log])
async def submit_orders(stream):
    async for event in stream:
        iteration = 0
        await asyncio.sleep(0.2) 
        # There's a timing issue here
        while (any( x > 0 for x in CURRENT_INVENTORY.values() if bool(CURRENT_INVENTORY)) or not bool(CURRENT_INVENTORY)):
            if (not bool(CURRENT_INVENTORY)):
                continue
            # Preferably a UUID but, following the example.
            iteration += 1
            message = {
                'Header': iteration,
                'Lines': []
            }
            # convert these to the ASCII code and then back with chr()
            for product in range(ord('A'), ord('F')):
                qty = randrange(5)
                if (qty == 0):
                    continue
                message['Lines'].append({"Product": chr(product), "Quantity": qty})
            if len(message['Lines']) == 0:
                continue
            await asyncio.sleep(0.25) 
            if( all( x < 0 for x in CURRENT_INVENTORY.values())):
                break
            await ordering.send(value=message)
        yield 0

@app.task
async def on_started():
    # async with app.producer:
    for product, on_hand in STARTING_INVENTORY.items():
        await stock.send(value={"product":product, "on_hand":on_hand})


@app.command()
async def seed_orders():
    async with app.producer:
        await asyncio.sleep(1) 
        await seed.send(value='Starting Seed - {}'.format(UUID))
    return

@deprecated(reason='Always empty, topic looks to be relevant here.')
@app.command()
async def describe():
    async with app.producer:
        print (inventory_table.items())

if __name__ == '__main__':
    app.main()
