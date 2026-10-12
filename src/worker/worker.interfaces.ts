export interface BatchExecutionResult {
  batchId: number;
  writesCount: number;
  readsCount: number;
  deletesCount: number;
  durationMs: number;
  master: string;
  success: boolean;
  error?: string;
}
