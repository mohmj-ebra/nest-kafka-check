import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import {
  Kafka,
  logLevel,
  LogEntry,
  logCreator as KafkaLogCreator,
} from 'kafkajs';
import * as dns from 'dns/promises';
import * as net from 'net';

function mask(value?: string) {
  if (!value) return '';
  if (value.length <= 4) return '****';
  return value.slice(0, 2) + '****' + value.slice(-2);
}

async function tcpProbe(host: string, port: number, timeoutMs = 4000) {
  return new Promise<{ ok: boolean; error?: string }>((resolve) => {
    const socket = new net.Socket();

    const done = (ok: boolean, error?: string) => {
      try {
        socket.destroy();
      } catch {}
      resolve({ ok, error });
    };

    socket.setTimeout(timeoutMs);

    socket.once('connect', () => done(true));
    socket.once('timeout', () => done(false, `timeout after ${timeoutMs}ms`));
    socket.once('error', (e: any) => done(false, e?.message || String(e)));

    socket.connect(port, host);
  });
}

@Injectable()
export class KafkaHealthService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KafkaHealthService.name);

  private admin: ReturnType<Kafka['admin']> | undefined;

  async onModuleInit() {
    const brokersRaw = process.env.KAFKA_BROKERS || '';
    const brokers = brokersRaw.split(',').map((b) => b.trim()).filter(Boolean);

    const clientId = process.env.KAFKA_CLIENT_ID || 'nest-kafka-check';

    const username = process.env.KAFKA_USERNAME || '';
    const password = process.env.KAFKA_PASSWORD || '';

    const sslEnabled =
      (process.env.KAFKA_SSL || 'true').toLowerCase() === 'true';

    const mechanism =
      (process.env.KAFKA_SASL_MECHANISM || 'plain').toLowerCase();

    const connectionTimeout = Number(process.env.KAFKA_CONN_TIMEOUT_MS || 8000);
    const requestTimeout = Number(process.env.KAFKA_REQ_TIMEOUT_MS || 30000);
    const retries = Number(process.env.KAFKA_RETRIES || 6);
    const retryInitial = Number(process.env.KAFKA_RETRY_INITIAL_MS || 300);

    const debugKafkaJs =
      (process.env.KAFKA_DEBUG || 'true').toLowerCase() === 'true';

    // ---- Basic config print (safe) ----
    this.logger.log('================ Kafka Check Boot ================');
    this.logger.log(`clientId=${clientId}`);
    this.logger.log(`brokersRaw="${brokersRaw}"`);
    this.logger.log(`brokersCount=${brokers.length}`);
    this.logger.log(`ssl=${sslEnabled}`);
    this.logger.log(`sasl=${username ? mechanism : 'none'}`);
    this.logger.log(`username=${username ? mask(username) : '(empty)'}`);
    this.logger.log(`password=${password ? mask(password) : '(empty)'}`);
    this.logger.log(
      `timeouts: connectionTimeout=${connectionTimeout}ms requestTimeout=${requestTimeout}ms`,
    );
    this.logger.log(
      `retries: retries=${retries} initialRetryTime=${retryInitial}ms`,
    );
    this.logger.log(`kafkajsDebug=${debugKafkaJs}`);
    this.logger.log('==================================================');

    if (brokers.length === 0) {
      this.logger.error(
        '❌ KAFKA_BROKERS is empty. Example: "broker:9092" or "b1:9092,b2:9092"',
      );
      return;
    }

    // ---- Preflight checks: DNS + TCP ----
    for (const broker of brokers) {
      const [host, portStr] = broker.split(':');
      const port = Number(portStr || 9092);

      this.logger.log(`Preflight: broker=${broker}`);

      try {
        const addrs = await dns.lookup(host, { all: true });
        this.logger.log(
          `DNS OK: ${host} -> ${addrs.map((a) => a.address).join(', ')}`,
        );
      } catch (e: any) {
        this.logger.error(`DNS FAIL: ${host} -> ${e?.message || e}`);
      }

      const probe = await tcpProbe(host, port, 4000);
      if (probe.ok) {
        this.logger.log(`TCP OK: ${host}:${port} (connected)`);
      } else {
        this.logger.error(`TCP FAIL: ${host}:${port} (${probe.error})`);
        this.logger.error(
          'Likely reason: firewall/VPC routing/NetworkPolicy/wrong port/endpoint not reachable from the pod.',
        );
      }
    }

    // ---- KafkaJS internal logger -> Nest Logger ----
    const nestKafkaLogger: KafkaLogCreator = (kafkaLogLevel) => {
      return ({ namespace, level, label, log }: LogEntry) => {
        const msg = `[KafkaJS][${label}][${namespace}] ${log.message}`;

        // Add extra context if present (avoid printing secrets)
        const extra = {
          broker: (log as any)?.broker,
          clientId: (log as any)?.clientId,
          correlationId: (log as any)?.correlationId,
          size: (log as any)?.size,
          error: (log as any)?.error
            ? {
                name: (log as any).error?.name,
                message: (log as any).error?.message,
                stackTop: String((log as any).error?.stack || '')
                  .split('\n')
                  .slice(0, 4)
                  .join(' | '),
              }
            : undefined,
        };

        // Map KafkaJS levels to Nest logs
        if (level >= logLevel.ERROR) {
          this.logger.error(msg, JSON.stringify(extra));
        } else if (level >= logLevel.WARN) {
          this.logger.warn(msg + ' ' + JSON.stringify(extra));
        } else if (debugKafkaJs) {
          // Only show info/debug if enabled
          this.logger.log(msg + ' ' + JSON.stringify(extra));
        }
      };
    };

    const kafka = new Kafka({
      clientId,
      brokers,
      ssl: sslEnabled,
      sasl: username
        ? {
            mechanism: mechanism as any,
            username,
            password,
          }
        : undefined,

      // Make KafkaJS more talkative + tweak connection behavior
      logLevel: debugKafkaJs ? logLevel.DEBUG : logLevel.ERROR,
      logCreator: nestKafkaLogger,

      connectionTimeout,
      requestTimeout,

      retry: {
        retries,
        initialRetryTime: retryInitial,
        maxRetryTime: 8000,
        factor: 0.2,
      },
    });

    this.admin = kafka.admin();

    // ---- Connect + list topics with strong error logging ----
    try {
      this.logger.log('⏳ Kafka admin.connect() ...');
      await this.admin.connect();
      this.logger.log('✅ Kafka admin connected');

      this.logger.log('⏳ Kafka admin.listTopics() ...');
      const topics = await this.admin.listTopics();
      this.logger.log(`✅ Kafka reachable. topics=${topics.length}`);

      // Optional: print first few topic names
      if (debugKafkaJs) {
        this.logger.log(`Topics sample: ${topics.slice(0, 10).join(', ')}`);
      }
    } catch (err: any) {
      this.logger.error('❌ Kafka connection FAILED');
      this.logger.error(
        `Error: ${err?.name || 'UnknownError'} - ${err?.message || err}`,
      );
      const stackTop = String(err?.stack || '')
        .split('\n')
        .slice(0, 8)
        .join('\n');
      if (stackTop) this.logger.error(`Stack(top):\n${stackTop}`);

      const msg = String(err?.message || '').toLowerCase();

      // More precise hints
      if (msg.includes('closed connection')) {
        this.logger.error(
          'Hint: broker is closing connection. Most common causes: TLS mismatch (ssl true/false wrong), wrong port/listener, SASL mismatch, or an L4 proxy/firewall resetting.',
        );
      }
      if (msg.includes('sasl') || msg.includes('authentication')) {
        this.logger.error(
          'Hint: SASL auth problem (wrong username/password, wrong mechanism, broker not configured for SASL).',
        );
      }
      if (
        msg.includes('ssl') ||
        msg.includes('tls') ||
        msg.includes('certificate') ||
        msg.includes('self signed')
      ) {
        this.logger.error(
          'Hint: TLS problem (ssl setting wrong or missing CA). If broker uses private CA, you must mount CA and pass it to KafkaJS ssl.ca.',
        );
      }
      if (msg.includes('enotfound') || msg.includes('getaddrinfo')) {
        this.logger.error(
          'Hint: DNS/hostname not resolvable from the pod (wrong broker host).',
        );
      }
      if (msg.includes('timed out') || msg.includes('etimedout')) {
        this.logger.error(
          'Hint: network timeout (firewall/VPC routing/NetworkPolicy/wrong port).',
        );
      }
      if (msg.includes('metadata') || msg.includes('leader')) {
        this.logger.error(
          'Hint: advertised.listeners issue (broker returns host/IP that pods cannot reach).',
        );
      }

      // Keep pod alive so you can exec into it / check DNS etc.
      // If you want it to crashloop instead, change to: process.exit(1)
      this.logger.warn('Keeping process alive for debugging...');
    }
  }

  async onModuleDestroy() {
    try {
      if (this.admin) {
        this.logger.log('Disconnecting Kafka admin...');
        await this.admin.disconnect();
        this.logger.log('Kafka admin disconnected');
      }
    } catch (e: any) {
      this.logger.warn(`Disconnect error: ${e?.message || e}`);
    }
  }
}
