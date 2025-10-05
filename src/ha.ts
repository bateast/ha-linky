import ws, { Message } from 'websocket';
import { MSG_TYPE_AUTH_INVALID, MSG_TYPE_AUTH_OK, MSG_TYPE_AUTH_REQUIRED } from 'home-assistant-js-websocket';
import { auth } from 'home-assistant-js-websocket/dist/messages.js';
import { debug, warn } from './log.js';
import dayjs from 'dayjs';
import { StatisticDataPoint } from './format.js';

const WS_URL = process.env.WS_URL || 'ws://supervisor/core/websocket';
const TOKEN = process.env.SUPERVISOR_TOKEN;

export type SuccessMessage = {
  id: string;
  type: string;
  success: true;
  result: any;
};

export type ErrorMessage = {
  id: string;
  type: string;
  success: false;
  error: any;
};

export type ResultMessage = SuccessMessage | ErrorMessage;

export type Hole = { from: dayjs.Dayjs; to: dayjs.Dayjs; lastSum: number; lastCost: number; next: string };

export class HomeAssistantClient {
  private messageId = Number(Date.now().toString().slice(9));
  private connection: ws.connection;

  public connect(): Promise<void> {
    return new Promise((resolve, reject) => {
      const client: ws.client = new ws.client();

      client.addListener('connectFailed', function (error) {
        reject('Connection with Home Assistant failed : ' + error.toString());
      });
      client.addListener('connect', (connection: ws.connection) => {
        connection.on('error', (error) => {
          reject('Connection with Home Assistant returned an error : ' + error.toString());
        });

        connection.on('close', () => {
          debug('Connection with Home Assistant closed');
        });

        connection.once('message', (message: Message) => {
          if (message.type === 'utf8' && JSON.parse(message.utf8Data).type === MSG_TYPE_AUTH_REQUIRED) {
            connection.once('message', (message) => {
              if (message.type === 'utf8') {
                const parsed: { type: string } = JSON.parse(message.utf8Data);
                if (parsed.type === MSG_TYPE_AUTH_INVALID) {
                  reject('Cannot authenticate with Home Assistant');
                }
                if (parsed.type === MSG_TYPE_AUTH_OK) {
                  debug('Connection with Home Assistant established');
                  this.connection = connection;
                  resolve();
                }
              }
            });

            connection.sendUTF(JSON.stringify(auth(TOKEN)));
          }
        });
      });
      client.connect(WS_URL);
    });
  }

  public disconnect() {
    this.connection.close();
  }

  private sendMessage(message: { [key: string]: any }): Promise<SuccessMessage> {
    message.id = this.messageId++;
    return new Promise((resolve, reject) => {
      this.connection.once('message', (message: Message) => {
        if (message.type === 'utf8') {
          const response: ResultMessage = JSON.parse(message.utf8Data);
          if (response.success) {
            resolve(response);
          } else {
            reject('Home Assistant returned an error : ' + message.utf8Data);
          }
        }
      });
      this.connection.sendUTF(JSON.stringify(message));
    });
  }

  public getStatisticId(args: { prm: string; isProduction: boolean; isCost?: boolean }): string {
    const { prm, isProduction, isCost } = args;
    return `${isProduction ? 'linky_prod' : 'linky'}:${prm}${isCost ? '_cost' : ''}`;
  }

  public async saveStatistics(args: {
    prm: string;
    name: string;
    isProduction: boolean;
    isCost?: boolean;
    stats: StatisticDataPoint[];
  }) {
    const { prm, name, isProduction, isCost, stats } = args;
    const statisticId = this.getStatisticId({ prm, isProduction, isCost });

    await this.sendMessage({
      type: 'recorder/import_statistics',
      metadata: {
        has_mean: false,
        has_sum: true,
        name: isCost ? `${name} (costs)` : name,
        source: statisticId.split(':')[0],
        statistic_id: statisticId,
        unit_of_measurement: isCost ? '€' : 'Wh',
      },
      stats,
    });
  }

  public async adjustSum(args: { prm: string; date: string; value: number; isProduction: boolean; isCost?: boolean }) {
    const { prm, date, value, isProduction, isCost } = args;
    const statisticId = this.getStatisticId({ prm, isProduction, isCost });

    await this.sendMessage({
      type: 'recorder/adjust_sum_statistics',
      statistic_id: statisticId,
      start_time: date,
      adjustment: value,
      adjustment_unit_of_measurement: isCost ? '€' : 'Wh',
    });
  }

  public async isNewPRM(args: { prm: string; isProduction: boolean; isCost?: boolean }) {
    const statisticId = this.getStatisticId(args);
    const ids = await this.sendMessage({
      type: 'recorder/list_statistic_ids',
      statistic_type: 'sum',
    });
    return !ids.result.find((statistic: any) => statistic.statistic_id === statisticId);
  }

  public async findLastStatistic(args: { prm: string; isProduction: boolean; isCost?: boolean }): Promise<null | {
    start: number;
    end: number;
    state: number;
    sum: number;
    change: number;
  }> {
    const { prm, isProduction, isCost } = args;
    const isNew = await this.isNewPRM({ prm, isProduction, isCost });
    if (isNew) {
      if (!isCost) {
        warn(`PRM ${prm} not found in Home Assistant statistics`);
      }
      return null;
    }

    const statisticId = this.getStatisticId({ prm, isProduction, isCost });

    // Loop over the last 52 weeks
    for (let i = 0; i < 52; i++) {
      const data = await this.sendMessage({
        type: 'recorder/statistics_during_period',
        start_time: dayjs()
          .subtract((i + 1) * 7, 'days')
          .format('YYYY-MM-DDT00:00:00.00Z'),
        end_time: dayjs()
          .subtract(i * 7, 'days')
          .format('YYYY-MM-DDT00:00:00.00Z'),
        statistic_ids: [statisticId],
        period: 'day',
      });
      const points = data.result[statisticId];
      if (points && points.length > 0) {
        const lastDay = dayjs(points[points.length - 1].start).format('DD/MM/YYYY');
        debug('Last saved statistic date is ' + lastDay);
        return points[points.length - 1];
      }
    }

    debug(`No statistics found for PRM ${prm} in Home Assistant`);
    return null;
  }

  /* -------------------------------------------------
     Helper – find the oldest statistic (or null)
     ------------------------------------------------- */
  private async findOldestStatistic(statisticId: string): Promise<StatisticDataPoint | null> {
    // Scan the last 3*52 weeks backwards; stop at the first block that contains data.
    for (let i = 3 * 51; i >= 0; i--) {
      const start = dayjs()
        .subtract((i + 1) * 7, 'day')
        .startOf('day')
        .format('YYYY-MM-DDTHH:mm:ss.SSS[Z]');
      const end = dayjs()
        .subtract(i * 7, 'day')
        .startOf('day')
        .format('YYYY-MM-DDTHH:mm:ss.SSS[Z]');

      const resp = await this.sendMessage({
        type: 'recorder/statistics_during_period',
        start_time: start,
        end_time: end,
        statistic_ids: [statisticId],
        period: 'day',
      });

      const points = (resp as SuccessMessage).result[statisticId] as StatisticDataPoint[] | undefined;

      if (points && points.length > 0) {
        // Return the earliest point in this block (the oldest overall)
        return points[0];
      }
    }
    return null; // no data at all
  }

  public async fetchLastSum(day: dayjs.Dayjs, statisticId: string): Promise<null | number> {
    const resp = await this.sendMessage({
      type: 'recorder/statistics_during_period',
      start_time: day.subtract(1, 'day').format('YYYY-MM-DDTHH:mm:ss.SSS[Z]'),
      end_time: day.format('YYYY-MM-DDTHH:mm:ss.SSS[Z]'),
      statistic_ids: [statisticId],
      period: 'day',
    });

    const points = (resp as SuccessMessage).result[statisticId] as StatisticDataPoint[] | undefined;

    if (points && points.length > 0) {
      // `sum` is the cumulative value stored by Home Assistant
      return points[points.length - 1].sum;
    }

    // No prior data
    return null;
  }

  /* -------------------------------------------------
     Public async generator – missing‑day holes
     ------------------------------------------------- */
  public async *missingDayRanges(args: {
    prm: string;
    isProduction: boolean;
    /** optional ISO‑8601 start day (UTC, 00:00) */
    startDay?: string;
  }): AsyncGenerator<Hole, void, unknown> {
    const { prm, isProduction, startDay } = args;
    const statisticId = this.getStatisticId({ prm, isProduction });

    let cursor: dayjs.Dayjs;
    if (startDay) {
      cursor = dayjs(startDay).startOf('day');
    } else {
      const oldest = await this.findOldestStatistic(statisticId);
      if (!oldest) {
        // No statistics at all → exit immediately
        return;
      }
      cursor = dayjs(oldest.start).add(1, 'day').startOf('day');
    }

    const today = dayjs().startOf('day');

    // Walk forward, yielding holes
    while (cursor.isBefore(today)) {
      const start = cursor.format('YYYY-MM-DDTHH:mm:ss.SSS[Z]');
      const end = cursor; // .add(1, 'day').format('YYYY-MM-DDTHH:mm:ss.SSS[Z]');

      const resp = await this.sendMessage({
        type: 'recorder/statistics_during_period',
        start_time: start,
        end_time: end,
        statistic_ids: [statisticId],
        period: 'day',
      });

      const points = (resp as SuccessMessage).result[statisticId] as StatisticDataPoint[] | undefined;

      if (!points || points.length === 0) {
        // ---- start a hole -------------------------------------------------
        const holeStart = cursor.clone();

        // extend until a day with data is found
        while (true) {
          cursor = cursor.add(1, 'day');
          if (cursor.isAfter(today)) break;

          const nxtStart = cursor.format('YYYY-MM-DDTHH:mm:ss.SSS[Z]');
          const nxtEnd = cursor.add(1, 'day').format('YYYY-MM-DDTHH:mm:ss.SSS[Z]');

          const nxtResp = await this.sendMessage({
            type: 'recorder/statistics_during_period',
            start_time: nxtStart,
            end_time: nxtEnd,
            statistic_ids: [statisticId],
            period: 'day',
          });

          const nxtPoints = (nxtResp as SuccessMessage).result[statisticId] as StatisticDataPoint[] | undefined;

          if (nxtPoints && nxtPoints.length > 0) {
            // hole ends right before this day
            const sum = await this.fetchLastSum(holeStart, statisticId);
            const next = dayjs(nxtPoints[0].start).format();
            const costs = await this.fetchLastSum(
              holeStart,
              this.getStatisticId({ prm: prm, isProduction, isCost: true }),
            );

            yield { from: holeStart, to: cursor, lastSum: !sum ? 0 : sum, lastCost: costs, next: next };
            break;
          }
        }
      } else {
        // day has data → move to next day
        cursor = cursor.add(1, 'day');
      }
    }
  }

  /* -------------------------------------------------
     Example helper – log all missing ranges
     ------------------------------------------------- */
  public async logMissingRanges(params: { prm: string; isProduction: boolean; isCost?: boolean; startDay?: string }) {
    for await (const hole of this.missingDayRanges(params)) {
      debug(`Missing statistics from ${hole.from} to ${hole.to} (last sum: ${hole.lastSum})`);
    }
  }

  public async purge(prm: string, isProduction: boolean) {
    const statisticId = this.getStatisticId({ prm, isProduction, isCost: false });
    const statisticIdWithCost = this.getStatisticId({ prm, isProduction, isCost: true });

    warn(`Removing all statistics for PRM ${prm}`);
    await this.sendMessage({
      type: 'recorder/clear_statistics',
      statistic_ids: [statisticId, statisticIdWithCost],
    });
  }
}
