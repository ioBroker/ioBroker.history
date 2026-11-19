/**
 * The answer of the message `help`: what the instance answers, written for an AI agent (e.g. ioBroker.pi-agent, or
 * the MCP server with `send_to`) and for script authors. Keep it in sync with `processMessage` in main.ts.
 */

/** Every command `processMessage` answers; `write` = it changes data, settings or the instance */
export const COMMANDS: { command: string; write: boolean }[] = [
    { command: 'help', write: false },
    { command: 'getHistory', write: false },
    { command: 'getEnabledDPs', write: false },
    { command: 'getDatapoints', write: false },
    { command: 'getRawEntries', write: false },
    { command: 'getDpStatistics', write: false },
    { command: 'features', write: false },
    { command: 'enableHistory', write: true },
    { command: 'disableHistory', write: true },
    { command: 'storeState', write: true },
    { command: 'update', write: true },
    { command: 'delete', write: true },
    { command: 'deleteRange', write: true },
    { command: 'deleteAll', write: true },
    { command: 'cleanupOrphaned', write: true },
    { command: 'stopInstance', write: true },
];

export const HELP = `ioBroker.history: stores the history of the states that are enabled for it (per state in the object settings)
in JSON files on the disk, one file per state and day; new values stay in RAM first. An agent can read the course
of a state (the stored values or one value per interval), list the logged states and the stored data and, only on
the user's request, enable or disable the logging and store, change or delete values.

Times (start, end, ts): milliseconds since 1970 or a date text such as "2026-11-19T08:00" (local time of the host).
Times in the answers are milliseconds since 1970.

Commands (sendTo history.0):
- getHistory {id, options: {start?, end?, aggregate?, count?, step?, ...}}: the course of one state:
  {result: [{ts, val, ack?, from?, q?, id?}], step, error}. options is required (may be {}).
  aggregate: average (default), min, max, minmax (min and max of each interval), total, count, percentile
  {percentile 0-100, default 50}, quantile {quantile 0-1, default 0.5}, integral {integralUnit in s, default 60,
  3600 gives Wh from W; integralInterpolation none (default) | linear}: one value per interval;
  none or onchange: the stored values. id "*" with none or onchange: the values of all logged states, with id.
  start: without it (or 0) the newest count values are returned, not aggregated. end: default now + 5000 s.
  count: the number of intervals (default 500), with none/onchange the number of values (default limit).
  step: the interval in ms instead of count. intervalBorders: [ms, ...] ascending borders of the intervals
  (e.g. local days), replaces start, end and step. limit: at most this many intervals or values (default
  count, else the instance setting, else 2000). returnNewestEntries: true = the newest values instead of the
  oldest when count cuts.
  ack, from, q, addId: true adds these fields. round: digits after the decimal point. ignoreNull: false
  (default, null stays), true (replaced by the value before), 0 (replaced by 0). removeBorderValues: true drops
  the values before start and after end that are added for charts.
- getEnabledDPs {}: the states this instance logs: {[id]: their settings (enabled, changesOnly, debounceTime,
  blockTime, changesMinDelta, changesRelogInterval, retention in s, maxLength, aliasId, ...)}.
- getDatapoints {}: every state with stored values, also the ones no longer logged:
  {success, result: [{id, type: Number | String | Boolean | null}]}.
- getRawEntries {id, start?, end?, limit? (1-5000, default 100), offset? (default 0), sort? desc (default) | asc,
  noTotal?}: the values exactly as stored (no aggregation, no border values):
  {id, total, truncated, limit, offset, sort, result: [{ts, val, ack?, q?, from?, user?, c?}]}. total counts at
  most 100000 values (then truncated is true); noTotal: true skips the counting.
- getDpStatistics {start?, end?}: per state the number of values and the disk usage (reads every file, takes a
  while): {success, result: [{id, type, count, firstTs, lastTs, bytes, files, status}], summary: {datapoints,
  values, bytes, byStatus}}. status: active, loggingDisabled (the object exists, not logged), objectMissing
  (the object was deleted).
- features {}: {supportedFeatures: ["update", "delete", "deleteRange", "deleteAll", "storeState"]}.
- enableHistory {id, options?}: logs the state from now on; options = its settings as in getEnabledDPs (the
  defaults of the instance otherwise): {success} or {error}.
- disableHistory {id}: stops logging the state, the stored values stay: {success} or {error}.
- storeState {id, state: {ts, val, ack?, q?, from?} or a list of them, rules?}: writes values into the history,
  also of a state that is not logged; rules: true applies the logging settings (only for a logged state):
  {success, successCount} or {error, errors, successCount}.
- update [{id, state: {ts, val?, ack?, q?, from?, user?, c?}}] or {id, state}: changes the stored value with
  exactly this ts: {success}.
- delete [{id, ts}] or {id, ts} or {id, ts: [ts, ...]}: deletes the values with exactly these ts: {success}
  (a single entry: success false = not found).
- deleteRange {id, start, end?} or [{id, start, end?}]: deletes the values with start <= ts <= end (end default
  now): {success}. ts, start and end: ms or a date string, 0 is valid. If one entry has no valid ts or start,
  nothing is deleted: {success: false, error} says what is missing.
- deleteAll [{id}] or {id}: deletes every stored value of these states: {success}.
- cleanupOrphaned {scope?: {objectMissing? (default true), loggingDisabled? (default false)}, confirm?}: the
  values of states that are not logged any more. Without confirm: true only the preview {dryRun: true,
  datapoints, values, bytes, items}; with confirm: true it deletes their files and adds {deleted, failed}.
- stopInstance: sent by js-controller before it stops the instance, not for agents.

How to talk to the user:
- Find the state first (search the objects); name states by their name, give times in local time and values
  with their unit.
- A state has a history only if it is or was logged (getEnabledDPs, getDatapoints). For a longer period request
  one value per interval (e.g. aggregate average with count 24 to 200) instead of every stored value.
- delete, deleteRange, deleteAll, update, storeState, cleanupOrphaned, enableHistory, disableHistory and
  stopInstance change the stored values, the settings or the instance: never without the user's explicit
  request. Deleted values cannot be restored: name the state and the range and ask for confirmation first;
  show the preview of cleanupOrphaned (without confirm) before deleting.`;
