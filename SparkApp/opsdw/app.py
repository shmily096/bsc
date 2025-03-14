import sys
sys.path.append("/SparkApp")

import timeit
import time
import argparse
from os.path import abspath
from opsdw.logger.dw_logger import DWLogger
from opsdw.controller.ods_controller import ODSController
from opsdw.common.com_config import Layers

def parse_args():
    argument_parser = argparse.ArgumentParser(description="Please use Layer & Table name")
    argument_parser.add_argument("--layer", type=str, default='all')
    argument_parser.add_argument("--table", type=str, default='all')
    return argument_parser.parse_args()

if __name__ == "__main__":
    start_time = timeit.default_timer()
    logger = DWLogger().logger
    try:
        
        args = parse_args()
        layer_name = args.layer.upper()

        logger.info(f"Start syncing table {args.table} on layer {layer_name} ")

        match Layers[layer_name]:
            case Layers.ODS:
                ODSController(args.table).distribute()

            case Layers.ALL:
                logger.info(args.table)
            case _:
                raise ValueError("Incorrect layer")

    except BaseException as err:
        logger.error(f"Unexpected {err=}, {type(err)=}")
   
    end_time = timeit.default_timer()
    logger.info(f"Finish syncing table {args.table} on layer {layer_name} ")
    time_span = end_time - start_time
    logger.info(f"The total time is {time_span:.0f} seconds ({int(time_span/60):0>2d} minutes)")