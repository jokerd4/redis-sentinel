export interface SentinelNode {
  host: string;
  port: number;
}

export interface RedisNode {
  host: string;
  port: number;
}

export interface FailoverEventInfo {
  masterGroup: string;
  previousMaster: RedisNode | null;
  newMaster: RedisNode;
  downtimeMs: number;
  timestamp: string;
}
