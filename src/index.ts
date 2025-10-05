import { HomeAssistantClient } from './ha.js';
import { MeterConfig } from './config.js';
import { computeCosts } from './cost.js';
import { debug, error } from './log.js';
import { formatAsStatistics, LinkyDataPoint } from './format.js';

import util from 'node:util';
import dayjs from 'dayjs';

import { main } from './main.js';

export async function runFill(
  config: MeterConfig,
  haClient: HomeAssistantClient,
  rawData: LinkyDataPoint[],
  startDay?: string,
) {
  for await (const hole of haClient.missingDayRanges({
    prm: config.prm,
    isProduction: config.production,
    startDay,
  })) {
    debug(`Missing statistics from ${hole.from} to ${hole.to}  (last sum: ${hole.lastSum})`);
    const holeDataRaw = rawData.filter((pt) => {
      const ptStart = dayjs(pt.date);
      return !ptStart.isBefore(hole.from) && ptStart.isBefore(hole.to);
    });

    if (!holeDataRaw.length) continue;

    const holeStats = formatAsStatistics(holeDataRaw, hole.lastSum);
    debug(`Values: ${util.inspect(holeStats, { depth: null, colors: false })}`);

    await haClient.saveStatistics({
      prm: config.prm,
      name: config.name,
      isProduction: config.production,
      stats: holeStats,
    });

    // Need to adjust sum of existing statistics
    if (holeStats.length) {
      const delta = holeStats[holeStats.length - 1].sum - hole.lastSum;
      debug(`Adjusting ${hole.next} sum with ${delta}`);
      await haClient.adjustSum({
        prm: config.prm,
        date: hole.next,
        value: delta,
        isProduction: config.production,
      });
    }

    if (config.costs && hole.lastCost) {
      const costStats = computeCosts(holeStats, config.costs, hole.lastCost);
      if (!costStats.length) continue;

      debug(`Costs: ${util.inspect(costStats, { depth: null, colors: false })}`);
      await haClient.saveStatistics({
        prm: config.prm,
        name: config.name,
        isProduction: config.production,
        isCost: true,
        stats: costStats,
      });

      const deltaCost = costStats[costStats.length - 1].sum - hole.lastCost;
      debug(`Adjusting ${hole.next} Costs sum with ${deltaCost}`);
      await haClient.adjustSum({
        prm: config.prm,
        date: hole.next,
        value: deltaCost,
        isProduction: config.production,
        isCost: true,
      });
    }
  }
}

try {
  await main();
} catch (e) {
  error(e.toString());
  process.exit(1);
}
