/**
 * 作业状态口径（Jobs 页轮询/终态判断与测试共用）。
 * `preempted` 是运行态：被高优先级作业让出容量，释放后回到 `queued` 继续，
 * 因此仍要轮询、仍可取消，且不得当作终态（不落报告、不按失败计）。
 */
export const STATE_COLOR: Record<string, string> = {
  queued: 'blue',
  running: 'processing',
  paused: 'orange',
  preempted: 'gold',
  done: 'green',
  error: 'red',
  cancelled: 'magenta',
}

export const ACTIVE_STATES: readonly string[] = ['queued', 'running', 'paused', 'preempted']
export const TERMINAL_STATES: readonly string[] = ['done', 'error', 'cancelled']

export const isActiveState = (s?: string | null): boolean => !!s && ACTIVE_STATES.includes(s)
export const isTerminalState = (s?: string | null): boolean => !!s && TERMINAL_STATES.includes(s)
