import Redis from 'ioredis';

// Thrown when the write and its WAIT ended up on different connections
export class ConnectionChangedError extends Error {
  constructor() {
    super('Connection changed before the write was confirmed by replicas');
  }
}

interface PendingAck {
  epoch: number;
  resolve: () => void;
  reject: (err: Error) => void;
}

/**
 * Confirms writes with WAIT, so an acknowledged write survives a master crash
 * (Sentinel promotes the replica with the highest offset) and a partitioned master cannot acknowledge anything.
 *
 * WAIT blocks the connection, so concurrent writes share one WAIT instead of queueing behind each other.
 * WAIT only covers writes made on the same connection: after a reconnect it says nothing about earlier writes.
 */
export class ReplicaAckBatcher {
  // Incremented on every lost connection
  private connectionEpoch = 0;
  private pending: PendingAck[] = [];
  private inFlight = false;

  constructor(
    private readonly client: Redis,
    private readonly replicas: number,
    private readonly timeoutMs: number,
  ) {
    client.on('close', () => this.connectionEpoch++);
  }

  /** Must be called right after the write resolved, before the connection could change. */
  confirm(): Promise<void> {
    const epoch = this.connectionEpoch;
    return new Promise<void>((resolve, reject) => {
      this.pending.push({ epoch, resolve, reject });
      void this.flush();
    });
  }

  private async flush(): Promise<void> {
    if (this.inFlight || this.pending.length === 0) return;
    this.inFlight = true;
    const batch = this.pending;
    this.pending = [];
    const epoch = this.connectionEpoch;

    try {
      const acked = await this.client.wait(this.replicas, this.timeoutMs);
      const sameConnection = epoch === this.connectionEpoch;
      for (const p of batch) {
        if (!sameConnection || p.epoch !== epoch) {
          p.reject(new ConnectionChangedError());
        } else if (acked < this.replicas) {
          p.reject(new Error(`Write confirmed by ${acked}/${this.replicas} replicas within ${this.timeoutMs}ms`));
        } else {
          p.resolve();
        }
      }
    } catch (err) {
      for (const p of batch) p.reject(err as Error);
    } finally {
      this.inFlight = false;
      void this.flush();
    }
  }
}
