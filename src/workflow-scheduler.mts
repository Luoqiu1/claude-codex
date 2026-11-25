import type {
  WorkflowLease,
  WorkflowState,
  WorkflowTask,
  WorkflowTaskStatus,
} from './workflow-state.mjs'

export interface WorkflowSchedulerOptions {
  readonly now: Date
  readonly workerId: string
  readonly leaseTtlMs: number
}

// The claim `schedule` handed a worker: its lease's worker id and the task's
// attempt count at that point. The attempt tells a worker's own later reclaim
// of the same task apart from the claim it lost.
export interface WorkflowClaim {
  readonly workerId: string
  readonly attempt?: number | undefined
}

export type WorkflowScheduleDecision =
  | { readonly type: 'claimed'; readonly task: WorkflowTask; readonly state: WorkflowState }
  | { readonly type: 'idle'; readonly reason: 'no-runnable-task'; readonly state: WorkflowState }

export class WorkflowTransitionError extends Error {
  override readonly name = 'WorkflowTransitionError'
  readonly taskId: string
  readonly from: WorkflowTaskStatus
  readonly to: WorkflowTaskStatus

  constructor(taskId: string, from: WorkflowTaskStatus, to: WorkflowTaskStatus) {
    super(`workflow task ${taskId} cannot transition from ${from} to ${to}`)
    this.taskId = taskId
    this.from = from
    this.to = to
  }
}

export class WorkflowTaskNotFoundError extends Error {
  override readonly name = 'WorkflowTaskNotFoundError'
  readonly taskId: string

  constructor(taskId: string) {
    super(`workflow task ${taskId} does not exist`)
    this.taskId = taskId
  }
}

export class WorkflowClaimError extends Error {
  override readonly name = 'WorkflowClaimError'
  readonly taskId: string
  readonly claim: WorkflowClaim

  constructor(task: WorkflowTask, claim: WorkflowClaim) {
    super(
      `workflow task ${task.id} is no longer claimed by ${describeClaim(claim)}; ${describeHolder(task)}`,
    )
    this.taskId = task.id
    this.claim = claim
  }
}

export function scheduleNextWorkflowTask(
  state: WorkflowState,
  options: WorkflowSchedulerOptions,
): WorkflowScheduleDecision {
  const current = failExpiredFinalAttempts(state, options.now)
  const task = runnableTasks(current.tasks, options.now)[0]
  if (!task) return { type: 'idle', reason: 'no-runnable-task', state: current }
  const claimed = startWorkflowTask(task, options)
  return { type: 'claimed', task: claimed, state: replaceTask(current, claimed) }
}

export function renewWorkflowLease(
  state: WorkflowState,
  taskId: string,
  options: WorkflowSchedulerOptions,
  attempt?: number,
): WorkflowState {
  const task = requireTask(state, taskId)
  if (!holdsClaim(task, { workerId: options.workerId, attempt })) return state
  return replaceTask(state, {
    ...task,
    updatedAt: options.now.toISOString(),
    lease: newLease(options),
  })
}

// With a claim, complete, fail and block refuse a task that claim no longer
// holds, such as one another worker reclaimed after its lease expired. Without
// one they apply to whatever attempt holds the task, for manual repair.
export function completeWorkflowTask(
  state: WorkflowState,
  taskId: string,
  at: Date,
  claim?: WorkflowClaim,
): WorkflowState {
  return replaceTask(state, transitionTask(claimedTask(state, taskId, claim), 'done', at))
}

export function failWorkflowTask(
  state: WorkflowState,
  taskId: string,
  errorMessage: string,
  at: Date,
  claim?: WorkflowClaim,
): WorkflowState {
  const task = claimedTask(state, taskId, claim)
  const nextStatus = task.attempts >= task.maxAttempts ? 'failed' : 'queued'
  return replaceTask(state, {
    ...transitionTask(task, nextStatus, at),
    lastError: errorMessage,
  })
}

export function blockWorkflowTask(
  state: WorkflowState,
  taskId: string,
  reason: string,
  at: Date,
  claim?: WorkflowClaim,
): WorkflowState {
  return replaceTask(state, {
    ...transitionTask(claimedTask(state, taskId, claim), 'blocked', at),
    blockedReason: reason,
  })
}

export function enqueueWorkflowTask(state: WorkflowState, task: WorkflowTask): WorkflowState {
  if (state.tasks.some((candidate) => candidate.id === task.id)) return replaceTask(state, task)
  return { ...state, tasks: [...state.tasks, task] }
}

function startWorkflowTask(task: WorkflowTask, options: WorkflowSchedulerOptions): WorkflowTask {
  if (task.status === 'running') {
    return {
      ...task,
      updatedAt: options.now.toISOString(),
      attempts: task.attempts + 1,
      lease: newLease(options),
      blockedReason: null,
    }
  }
  return {
    ...transitionTask(task, 'running', options.now),
    attempts: task.attempts + 1,
    lease: newLease(options),
    blockedReason: null,
  }
}

function transitionTask(task: WorkflowTask, status: WorkflowTaskStatus, at: Date): WorkflowTask {
  if (!canTransition(task.status, status)) {
    throw new WorkflowTransitionError(task.id, task.status, status)
  }
  return {
    ...task,
    status,
    updatedAt: at.toISOString(),
    lease: status === 'running' ? task.lease : null,
  }
}

function canTransition(from: WorkflowTaskStatus, to: WorkflowTaskStatus): boolean {
  switch (from) {
    case 'queued':
      return to === 'running' || to === 'blocked' || to === 'done' || to === 'failed'
    case 'running':
      return to === 'queued' || to === 'blocked' || to === 'done' || to === 'failed'
    case 'blocked':
      return to === 'queued' || to === 'failed'
    case 'done':
    case 'failed':
      return false
    default:
      return assertNever(from)
  }
}

function failExpiredFinalAttempts(state: WorkflowState, now: Date): WorkflowState {
  let tasks: WorkflowTask[] | undefined
  for (const [index, task] of state.tasks.entries()) {
    if (task.attempts < task.maxAttempts || !hasExpiredLease(task, now)) continue
    tasks ??= [...state.tasks]
    tasks[index] = {
      ...transitionTask(task, 'failed', now),
      lastError: 'Worker lease expired on the final attempt',
    }
  }
  return tasks ? { ...state, tasks } : state
}

function runnableTasks(tasks: readonly WorkflowTask[], now: Date): readonly WorkflowTask[] {
  return tasks
    .filter((task) => task.status === 'queued' || hasExpiredLease(task, now))
    .filter((task) => task.attempts < task.maxAttempts)
    .toSorted(
      (left, right) =>
        right.priority - left.priority || left.updatedAt.localeCompare(right.updatedAt),
    )
}

function hasExpiredLease(task: WorkflowTask, now: Date): boolean {
  return (
    task.status === 'running' &&
    task.lease !== null &&
    Date.parse(task.lease.expiresAt) <= now.getTime()
  )
}

function newLease(options: WorkflowSchedulerOptions): WorkflowLease {
  return {
    workerId: options.workerId,
    renewedAt: options.now.toISOString(),
    expiresAt: new Date(options.now.getTime() + options.leaseTtlMs).toISOString(),
  }
}

function replaceTask(state: WorkflowState, task: WorkflowTask): WorkflowState {
  return {
    ...state,
    tasks: state.tasks.map((candidate) => (candidate.id === task.id ? task : candidate)),
  }
}

function requireTask(state: WorkflowState, taskId: string): WorkflowTask {
  const task = state.tasks.find((candidate) => candidate.id === taskId)
  if (task) return task
  throw new WorkflowTaskNotFoundError(taskId)
}

function claimedTask(
  state: WorkflowState,
  taskId: string,
  claim: WorkflowClaim | undefined,
): WorkflowTask {
  const task = requireTask(state, taskId)
  if (claim && !holdsClaim(task, claim)) throw new WorkflowClaimError(task, claim)
  return task
}

// A claim whose lease expired still holds the task until another worker
// reclaims it; `schedule` then bumps the attempt and replaces the lease.
function holdsClaim(task: WorkflowTask, claim: WorkflowClaim): boolean {
  return (
    task.status === 'running' &&
    task.lease?.workerId === claim.workerId &&
    (claim.attempt === undefined || claim.attempt === task.attempts)
  )
}

function describeClaim(claim: WorkflowClaim): string {
  const attempt = claim.attempt === undefined ? '' : ` attempt ${claim.attempt}`
  return `worker ${claim.workerId}${attempt}`
}

function describeHolder(task: WorkflowTask): string {
  if (task.status !== 'running') return `it is ${task.status}`
  if (!task.lease) return `attempt ${task.attempts} is running without a lease`
  return `attempt ${task.attempts} is running for worker ${task.lease.workerId}`
}

function assertNever(value: never): never {
  return value
}
