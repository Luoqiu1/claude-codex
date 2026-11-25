import assert from 'node:assert/strict'
import { spawnSync } from 'node:child_process'
import { mkdtemp, readFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import test, { type TestContext } from 'node:test'
import { fileURLToPath } from 'node:url'
import { runWorkflowCli } from '../src/workflow-cli.mjs'
import type { WorkflowState, WorkflowTask } from '../src/workflow-state.mjs'

// #286: once worker A's lease expired and another claim took the next attempt,
// a late result from A must not settle, requeue or block that attempt.

const cliPath = fileURLToPath(new URL('../src/workflow-cli.mjs', import.meta.url))
const stale = /is no longer claimed by worker A( attempt 1)?; attempt 2 is running for worker (A|B)/

for (const [command, extra] of [
  ['complete', []],
  ['fail', ['--error', 'late failure from A']],
  ['block', ['--reason', 'late block from A']],
] as const) {
  for (const claim of [
    ['--worker', 'A'],
    ['--worker', 'A', '--attempt', '1'],
  ]) {
    test(`a late ${command} ${claim.join(' ')} leaves another worker's reclaim running`, async (t) => {
      const queue = await reclaimedQueue(t, 'B')
      const before = await readFile(queue.statePath)
      const io = silentIo()

      await assert.rejects(
        queue.run([command, '--task', 't', ...claim, ...extra, '--now', at(3)], io),
        stale,
      )

      assert.deepEqual(await readFile(queue.statePath), before)
      assert.equal(io.text(), '')
      const task = await queue.task()
      assert.equal(task.status, 'running')
      assert.equal(task.attempts, 2)
      assert.equal(task.lease?.workerId, 'B')
      assert.equal(task.lease?.expiresAt, '2026-11-15T00:01:02.000Z')
    })
  }
}

test('the attempt fences a worker that reclaimed its own task', async (t) => {
  const queue = await reclaimedQueue(t, 'A')
  const before = await queue.task()

  await assert.rejects(
    queue.run(['complete', '--task', 't', '--worker', 'A', '--attempt', '1', '--now', at(3)]),
    stale,
  )
  await queue.run(['heartbeat', '--task', 't', '--worker', 'A', '--attempt', '1', '--now', at(4)])
  assert.deepEqual(await queue.task(), before)

  await queue.run(['heartbeat', '--task', 't', '--worker', 'A', '--attempt', '2', '--now', at(5)])
  assert.equal((await queue.task()).lease?.expiresAt, '2026-11-15T00:30:05.000Z')
  await queue.run(['complete', '--task', 't', '--worker', 'A', '--attempt', '2', '--now', at(6)])
  const task = await queue.task()
  assert.equal(task.status, 'done')
  assert.equal(task.lease, null)
})

test('the current claim can fail and block its task', async (t) => {
  const queue = await reclaimedQueue(t, 'B')
  await queue.run([
    'fail',
    '--task',
    't',
    '--worker',
    'B',
    '--attempt',
    '2',
    '--error',
    'x',
    '--now',
    at(3),
  ])
  assert.equal((await queue.task()).status, 'queued')

  await queue.run(['schedule', '--worker', 'C', '--now', at(10)])
  await queue.run([
    'block',
    '--task',
    't',
    '--worker',
    'C',
    '--attempt',
    '3',
    '--reason',
    'y',
    '--now',
    at(11),
  ])
  const task = await queue.task()
  assert.equal(task.status, 'blocked')
  assert.equal(task.blockedReason, 'y')
})

test('a late result for a task that is no longer running names its status', async (t) => {
  const queue = await reclaimedQueue(t, 'B')
  await queue.run(['complete', '--task', 't', '--worker', 'B', '--attempt', '2', '--now', at(3)])

  await assert.rejects(
    queue.run(['complete', '--task', 't', '--worker', 'B', '--attempt', '2', '--now', at(4)]),
    /workflow task t is no longer claimed by worker B attempt 2; it is done/,
  )
})

test('without --worker, complete still settles the running attempt for manual repair', async (t) => {
  const queue = await reclaimedQueue(t, 'B')
  await queue.run(['complete', '--task', 't', '--now', at(3)])
  assert.equal((await queue.task()).status, 'done')
})

test('an unusable --attempt is refused before anything is written', async (t) => {
  const queue = await reclaimedQueue(t, 'B')
  const before = await readFile(queue.statePath)
  for (const [args, message] of [
    [['--worker', 'B', '--attempt', '0'], /--attempt must be a positive integer/],
    [['--worker', 'B', '--attempt', '1.5'], /--attempt must be a positive integer/],
    [['--attempt', '2'], /--attempt needs --worker/],
  ] as const) {
    await assert.rejects(queue.run(['complete', '--task', 't', ...args]), message)
  }
  assert.deepEqual(await readFile(queue.statePath), before)
})

test('the CLI exits 1 and explains a refused late result', async (t) => {
  const queue = await reclaimedQueue(t, 'B')
  const result = spawnSync(
    process.execPath,
    [
      cliPath,
      'complete',
      '--task',
      't',
      '--worker',
      'A',
      '--attempt',
      '1',
      '--state',
      queue.statePath,
    ],
    { encoding: 'utf8', timeout: 10000 },
  )

  assert.equal(result.error, undefined)
  assert.equal(result.status, 1)
  assert.equal(result.stdout, '')
  assert.match(result.stderr, /^\[claude-codex-workflow\] workflow task t is no longer claimed/)
  assert.equal((await queue.task()).lease?.workerId, 'B')
})

// Worker A claims attempt 1 with a 1s lease; `reclaimer` takes attempt 2 two
// seconds later with a 60s lease.
async function reclaimedQueue(t: TestContext, reclaimer: string) {
  const dir = await mkdtemp(join(tmpdir(), 'claude-codex-workflow-stale-'))
  t.after(() => rm(dir, { recursive: true, force: true }))
  const statePath = join(dir, 'state.json')
  const run = (args: readonly string[], io = silentIo()) =>
    runWorkflowCli([...args, '--state', statePath], io)
  const queue = {
    statePath,
    run,
    task: async (): Promise<WorkflowTask> => {
      const state = JSON.parse(await readFile(statePath, 'utf8')) as WorkflowState
      const task = state.tasks.find((candidate) => candidate.id === 't')
      assert.ok(task)
      return task
    },
  }
  await run([
    'enqueue',
    '--id',
    't',
    '--prompt',
    'synthetic',
    '--max-attempts',
    '3',
    '--now',
    at(0),
  ])
  await run(['schedule', '--worker', 'A', '--ttl-ms', '1000', '--now', at(0)])
  await run(['schedule', '--worker', reclaimer, '--ttl-ms', '60000', '--now', at(2)])
  const claimed = await queue.task()
  assert.equal(claimed.attempts, 2)
  assert.equal(claimed.lease?.workerId, reclaimer)
  return queue
}

function at(seconds: number): string {
  return `2026-11-15T00:00:${String(seconds).padStart(2, '0')}Z`
}

function silentIo() {
  let stdout = ''
  return {
    stdout: {
      write: (chunk: string) => {
        stdout += chunk
        return true
      },
    },
    stderr: { write: () => true },
    text: () => stdout,
  }
}
