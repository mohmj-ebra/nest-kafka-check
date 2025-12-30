import { Injectable, Logger, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { Kafka, logLevel } from 'kafkajs';

@Injectable()
export class KafkaHealthService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KafkaHealthService.name);

  private kafka?: Kafka;
  private admin: any; // kafkajs Admin type (kept simple)

  async onModuleInit() {
    const brokersRaw = process.env.KAFKA_BROKERS || '';
    const brokers = brokersRaw
      .split(',')
      .map(b => b.trim())
      .filter(Boolean);

    const username = process.env.KAFKA_USERNAME || '';
    const password = process.env.KAFKA_PASSWORD || '';

    const sslEnabled = (process.env.KAFKA_SSL || 'true').toLowerCase() === 'true';
    const saslMechanism = (process.env.KAFKA_SASL_MECHANISM || 'plain').toLowerCase(); // plain | scram-sha-256 | scram-sha-512

    if (brokers.length === 0) {
      this.logger.error('❌ KAFKA_BROKERS is empty. Example: "kafka:9092" or "broker1:9092,broker2:9092"');
      return;
    }

    if (!username || !password) {
      this.logger.warn('⚠️ KAFKA_USERNAME/KAFKA_PASSWORD are empty. If your Kafka requires auth, connection will fail.');
    }

    this.logger.log(`Kafka config: brokers=${brokers.join(',')} ssl=${sslEnabled} sasl=${username ? saslMechanism : 'none'}`);

    this.kafka = new Kafka({
      clientId: process.env.KAFKA_CLIENT_ID || 'nest-kafka-check',
      brokers,
      ssl: sslEnabled,
      sasl: username
        ? {
            mechanism: saslMechanism as any,
            username,
            password,
          }
        : undefined,
      logLevel: logLevel.NOTHING,
    });

    this.admin = this.kafka.admin();

    // Try to connect and list topics to confirm it works end-to-end
    try {
      this.logger.log('⏳ Connecting to Kafka...');
      await this.admin.connect();
      this.logger.log('✅ Kafka admin connected successfully');

      const topics = await this.admin.listTopics();
      this.logger.log(`✅ Kafka reachable. Topics count: ${topics.length}`);
    } catch (err: any) {
      this.logger.error('❌ Kafka connection FAILED');
      this.logger.error(`Error: ${err?.name || 'UnknownError'} - ${err?.message || err}`);

      // Helpful “what could be the reason” mapping
      const msg = String(err?.message || '').toLowerCase();

      if (msg.includes('sasl') || msg.includes('authentication') || msg.includes('not authorized')) {
        this.logger.error('Likely reason: SASL auth failed (wrong username/password, wrong mechanism, or broker not configured for SASL).');
      }
      if (msg.includes('ssl') || msg.includes('tls') || msg.includes('certificate') || msg.includes('self signed')) {
        this.logger.error('Likely reason: TLS/SSL mismatch (ssl=true/false wrong), missing CA cert, or broker uses a cert not trusted by container.');
      }
      if (msg.includes('getaddrinfo') || msg.includes('enotfound') || msg.includes('name resolution')) {
        this.logger.error('Likely reason: DNS/hostname problem (broker hostname not resolvable from pod / wrong service name).');
      }
      if (msg.includes('econnrefused') || msg.includes('timed out') || msg.includes('etimedout')) {
        this.logger.error('Likely reason: network connectivity issue (wrong port, firewall, security group, NetworkPolicy, broker not reachable).');
      }
      if (msg.includes('broker not available') || msg.includes('not a leader') || msg.includes('metadata')) {
        this.logger.error('Likely reason: advertised.listeners misconfigured (broker returns an address pods cannot reach).');
      }

      // Optional: exit non-zero so k8s restarts and you notice quickly
      process.exitCode = 1;
    }
  }

  async onModuleDestroy() {
    try {
      if (this.admin) {
        await this.admin.disconnect();
        this.logger.log('Kafka admin disconnected');
      }
    } catch {
      // ignore
    }
  }
}
