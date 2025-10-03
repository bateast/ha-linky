import { HomeAssistantClient } from './ha.js';
import { LinkyClient } from './linky.js';
import { getUserConfig, MeterConfig } from './config.js';
import { getMeterHistory } from './history.js';
import { incrementSums } from './format.js';
import { computeCosts } from './cost.js';
import { debug, error, info, warn } from './log.js';
import { formatAsStatistics } from './format.js';

import util from 'node:util';
import cron from 'node-cron';
import dayjs from 'dayjs';

async function main() {
  debug('HA Linky is starting');

  const userConfig = getUserConfig();

  // Stop if configuration is empty
  if (userConfig.meters.length === 0) {
    warn('Add-on is not configured properly');
    debug('HA Linky stopped');
    return;
  }

  const haClient = new HomeAssistantClient();
  await haClient.connect();

  // Reset statistics if needed
  for (const config of userConfig.meters) {
    if (config.action === 'reset') {
      await haClient.purge(config.prm, config.production);
      info(`Statistics removed successfully for PRM ${config.prm} !`);
    }
    if (config.fill) {
      debug(`Looking for holes: ${config.fill}`);
      const client = new LinkyClient(config.token, config.prm, config.production);
      const raw_data = await client.getEnergyData(null);
      debug(`Raw energy: ${util.inspect(raw_data, { depth: null, colors: false })}`);

      for await (const hole of haClient.missingDayRanges({
        prm: config.prm,
        isProduction: config.production,
      })) {
        debug(`Missing statistics from ${hole.from} to ${hole.to}  (last sum: ${hole.lastSum})`);
        // keep points where start ≥ from && start < to
        const holeDataRaw = raw_data.filter((pt) => {
          const ptStart = dayjs(pt.date);
          const notBeforeFrom = !ptStart.isBefore(hole.from); // same as ≥ from
          const beforeTo = ptStart.isBefore(hole.to); // < to
          return notBeforeFrom && beforeTo;
        });

        if (holeDataRaw.length === 0) {
          continue;
        }
        const holeData = formatAsStatistics(holeDataRaw, hole.lastSum);
        debug(`Values: ${util.inspect(holeData, { depth: null, colors: false })}`);

        await haClient.saveStatistics({
          prm: config.prm,
          name: config.name,
          isProduction: config.production,
          stats: holeData,
        });
        if (0 < holeData.length) {
          const holeSum = holeData[holeData.length - 1].sum - hole.lastSum;
          debug(`Adjusting ${hole.next} sum with ${holeSum}`);
          await haClient.adjustSum({
            prm: config.prm,
            date: hole.next,
            value: holeSum,
            isProduction: config.production,
          });
        }
        if (config.costs && hole.lastCost) {
          const holeCosts = computeCosts(holeData, config.costs, hole.lastCost);

          if (holeCosts.length > 0) {
            debug(`Costs: ${util.inspect(holeCosts, { depth: null, colors: false })}`);
            await haClient.saveStatistics({
              prm: config.prm,
              name: config.name,
              isProduction: config.production,
              isCost: true,
              stats: holeCosts,
            });
            if (0 < holeCosts.length) {
              const holeSumCosts = holeCosts[holeCosts.length - 1].sum - hole.lastCost;
              debug(`Adjusting ${hole.next} Costs sum with ${holeSumCosts}`);
              await haClient.adjustSum({
                prm: config.prm,
                date: hole.next,
                value: holeSumCosts,
                isProduction: config.production,
                isCost: true,
              });
            }
          }
        }
      }
      haClient.disconnect();
      return;
    }
  }

  // Stop if nothing else to do
  if (userConfig.meters.every((config) => config.action !== 'sync')) {
    haClient.disconnect();
    info('Nothing to sync');
    debug('HA Linky stopped');
    return;
  }

  async function init(config: MeterConfig) {
    info(
      `[${dayjs().format('DD/MM HH:mm')}] New PRM detected, historical ${
        config.production ? 'production' : 'consumption'
      } data import is starting`,
    );

    let energyData = await getMeterHistory(config.prm);

    if (energyData.length === 0) {
      const client = new LinkyClient(config.token, config.prm, config.production);
      energyData = formatAsStatistics(await client.getEnergyData(null));
    }

    if (energyData.length === 0) {
      warn(`No history found for PRM ${config.prm}`);
      return;
    }

    await haClient.saveStatistics({
      prm: config.prm,
      name: config.name,
      isProduction: config.production,
      stats: energyData,
    });

    if (config.costs) {
      const costs = computeCosts(energyData, config.costs);
      if (costs.length > 0) {
        await haClient.saveStatistics({
          prm: config.prm,
          name: config.name,
          isProduction: config.production,
          isCost: true,
          stats: costs,
        });
      }
    }
  }

  async function sync(config: MeterConfig) {
    info(
      `[${dayjs().format('DD/MM HH:mm')}] Synchronization started for ${
        config.production ? 'production' : 'consumption'
      } data`,
    );

    const lastStatistic = await haClient.findLastStatistic({
      prm: config.prm,
      isProduction: config.production,
    });
    if (!lastStatistic) {
      warn(`Data synchronization failed, no previous statistic found in Home Assistant`);
      return;
    }

    const isSyncingNeeded = dayjs(lastStatistic.start).isBefore(dayjs().subtract(2, 'days')) && dayjs().hour() >= 6;
    if (!isSyncingNeeded) {
      debug('Everything is up-to-date, nothing to synchronize');
      return;
    }
    const client = new LinkyClient(config.token, config.prm, config.production);
    const firstDay = dayjs(lastStatistic.start).add(1, 'day');
    const energyData = formatAsStatistics(await client.getEnergyData(firstDay));
    await haClient.saveStatistics({
      prm: config.prm,
      name: config.name,
      isProduction: config.production,
      stats: incrementSums(energyData, lastStatistic.sum),
    });

    if (config.costs) {
      const costs = computeCosts(energyData, config.costs);
      if (costs.length > 0) {
        const lastCostStatistic = await haClient.findLastStatistic({
          prm: config.prm,
          isProduction: config.production,
          isCost: true,
        });
        await haClient.saveStatistics({
          prm: config.prm,
          name: config.name,
          isProduction: config.production,
          isCost: true,
          stats: incrementSums(costs, lastCostStatistic?.sum || 0),
        });
      }
    }
  }

  // Initialize or sync data
  for (const config of userConfig.meters) {
    if (config?.action === 'sync') {
      info(`PRM ${config.prm} found in configuration for ${config.production ? 'production' : 'consumption'}`);

      const isNew = await haClient.isNewPRM({
        prm: config.prm,
        isProduction: config.production,
      });
      if (isNew) {
        await init(config);
      } else {
        await sync(config);
      }
    }
  }

  haClient.disconnect();

  // Setup cron job
  const randomMinute = Math.floor(Math.random() * 59);
  const randomSecond = Math.floor(Math.random() * 59);

  info(
    `Data synchronization planned every day at ` +
      `06:${randomMinute.toString().padStart(2, '0')}:${randomSecond.toString().padStart(2, '0')} and ` +
      `09:${randomMinute.toString().padStart(2, '0')}:${randomSecond.toString().padStart(2, '0')}`,
  );

  cron.schedule(`${randomSecond} ${randomMinute} 6,9 * * *`, async () => {
    await haClient.connect();
    for (const config of userConfig.meters) {
      if (config.action === 'sync') {
        await sync(config);
      }
    }

    haClient.disconnect();
  });
}

try {
  await main();
} catch (e) {
  error(e.toString());
  process.exit(1);
}
