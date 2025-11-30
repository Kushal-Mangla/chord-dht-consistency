import asyncio
from simulation.simulator import Simulator

async def main():
    simulator = Simulator()
    await simulator.start()

if __name__ == "__main__":
    asyncio.run(main())