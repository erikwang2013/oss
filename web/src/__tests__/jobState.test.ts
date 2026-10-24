import { describe, expect, it } from 'vitest'
import { STATE_COLOR, isActiveState, isTerminalState } from '../jobState'

describe('作业状态口径', () => {
  it('preempted 有独立颜色，是运行态而非终态', () => {
    expect(STATE_COLOR.preempted).toBeTruthy()
    expect(isActiveState('preempted')).toBe(true)
    expect(isTerminalState('preempted')).toBe(false)
    // 每种状态颜色不同，避免与既有状态混淆
    expect(new Set(Object.values(STATE_COLOR)).size).toBe(Object.keys(STATE_COLOR).length)
  })

  it('既有状态映射不变', () => {
    expect(STATE_COLOR).toMatchObject({
      queued: 'blue',
      running: 'processing',
      paused: 'orange',
      done: 'green',
      error: 'red',
      cancelled: 'magenta',
    })
    for (const s of ['queued', 'running', 'paused']) expect(isActiveState(s)).toBe(true)
    for (const s of ['done', 'error', 'cancelled']) {
      expect(isTerminalState(s)).toBe(true)
      expect(isActiveState(s)).toBe(false)
    }
  })

  it('未知/空状态不误判', () => {
    expect(isActiveState(undefined)).toBe(false)
    expect(isActiveState(null)).toBe(false)
    expect(isTerminalState(undefined)).toBe(false)
    expect(isActiveState('mystery')).toBe(false)
    expect(isTerminalState('mystery')).toBe(false)
  })
})
