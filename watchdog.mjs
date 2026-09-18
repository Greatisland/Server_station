// ============================================================
// 서버 스테이션 (watchdog)
// - servers.config.json 에 등록된 서버들을 감시/제어한다
// - 3738 포트(기본)로 관제 UI(control.html)를 서빙한다
// - 서버 프로세스는 detached 로 띄우므로 스테이션이 닫혀도 서버는 유지된다
// - 서버는 자동으로 켜지지 않는다 — 관제 페이지에서 수동으로 [켜기] 해야 한다
// 실행: node watchdog.mjs   (설치 후에는 로그인 시 자동 실행됨)
// ============================================================
import http from 'node:http';
import net from 'node:net';
import fs from 'node:fs';
import path from 'node:path';
import { spawn, execFile } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const VERSION = '1.3.1';
const HERE = path.dirname(fileURLToPath(import.meta.url));
const CONFIG_PATH = path.join(HERE, 'servers.config.json');
const CONTROL_HTML = path.join(HERE, 'control.html');
const RUN_HIDDEN_VBS = path.join(HERE, 'run-hidden.vbs');
const LOG_DIR = path.join(HERE, 'logs');
const WATCHDOG_LOG = path.join(LOG_DIR, 'watchdog.log');
const PID_FILE = path.join(LOG_DIR, 'watchdog.pid');

const LOG_ROTATE_BYTES = 5 * 1024 * 1024;
const MONITOR_INTERVAL_MS = 5000;
const START_TIMEOUT_MS = 90_000;
const STOP_TIMEOUT_MS = 12_000;

const DEFAULT_CONFIG = {
  controlPort: 3738,
  servers: [
    {
      id: 'migmanager',
      name: 'MigManager',
      cwd: '..',
      command: 'npm run start',
      port: 3737,
      buildCommand: 'npm run build',
      builtMarker: '.next/BUILD_ID',
    },
  ],
};

// ---------- 공용 유틸 ----------
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const ts = () => new Date().toLocaleString('sv-SE');

function rotate(file) {
  try {
    if (fs.existsSync(file) && fs.statSync(file).size > LOG_ROTATE_BYTES) {
      const old = file + '.old';
      if (fs.existsSync(old)) fs.unlinkSync(old);
      fs.renameSync(file, old);
    }
  } catch { /* 로그 회전 실패는 치명적이지 않음 */ }
}

function wlog(msg) {
  const line = `[${ts()}] ${msg}`;
  console.log(line);
  try {
    rotate(WATCHDOG_LOG);
    fs.appendFileSync(WATCHDOG_LOG, line + '\n');
  } catch { /* ignore */ }
}

async function waitFor(fn, totalMs, stepMs) {
  const t0 = Date.now();
  while (Date.now() - t0 < totalMs) {
    if (await fn()) return true;
    await sleep(stepMs);
  }
  return false;
}

function tailFile(file, maxLines = 200) {
  try {
    const st = fs.statSync(file);
    const size = Math.min(st.size, 64 * 1024);
    if (size === 0) return '';
    const fd = fs.openSync(file, 'r');
    const buf = Buffer.alloc(size);
    fs.readSync(fd, buf, 0, size, st.size - size);
    fs.closeSync(fd);
    return buf.toString('utf8').split(/\r?\n/).slice(-maxLines).join('\n');
  } catch {
    return '';
  }
}

// ---------- 설정 ----------
let cfg = null;

function loadConfig() {
  if (!fs.existsSync(CONFIG_PATH)) {
    fs.writeFileSync(CONFIG_PATH, JSON.stringify(DEFAULT_CONFIG, null, 2));
    wlog('설정 파일이 없어 기본 설정을 생성했습니다.');
  }
  cfg = JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8'));
  if (!Number.isInteger(cfg.controlPort)) cfg.controlPort = 3738;
  if (!Array.isArray(cfg.servers)) cfg.servers = [];
}

function saveConfig() {
  const tmp = CONFIG_PATH + '.tmp';
  fs.writeFileSync(tmp, JSON.stringify(cfg, null, 2));
  fs.renameSync(tmp, CONFIG_PATH);
}

const resolveCwd = (s) => path.resolve(HERE, s.cwd || '.');
function logPath(id, type) {
  const primary = path.join(LOG_DIR, `${id}-${type}.log`);
  if (type !== 'run') return primary;
  try {
    const prefix = `${id}-${type}`;
    const newest = fs.readdirSync(LOG_DIR)
      .filter((f) => f.startsWith(prefix) && f.endsWith('.log') && !f.endsWith('.old'))
      .map((f) => ({ f, m: fs.statSync(path.join(LOG_DIR, f)).mtimeMs }))
      .sort((a, b) => b.m - a.m)[0];
    return newest ? path.join(LOG_DIR, newest.f) : primary;
  } catch {
    return primary;
  }
}

function slugify(name, taken) {
  let base = String(name).toLowerCase().replace(/[^a-z0-9가-힣]+/g, '-').replace(/^-+|-+$/g, '') || 'server';
  let id = base, n = 2;
  while (taken.has(id)) id = `${base}-${n++}`;
  return id;
}

// ---------- 서버 런타임 상태 ----------
// phase: unknown | stopped | starting | building | running | stopping | crashed | failed
const runtimes = new Map();

function getRt(id) {
  if (!runtimes.has(id)) {
    runtimes.set(id, {
      phase: 'unknown', opInFlight: null,
      startedAt: null, lastError: null, fails: 0, pid: null,
    });
  }
  return runtimes.get(id);
}

// ---------- 프로세스 제어 ----------
/** HTTP 응답이 오면 산 것으로 본다. Next·워커 내부 API용. */
function probeHttp(port, timeout = 3000) {
  return new Promise((resolve) => {
    const req = http.get({ host: '127.0.0.1', port, path: '/', timeout }, (res) => {
      res.resume();
      resolve(true);
    });
    req.on('timeout', () => { req.destroy(); resolve(false); });
    req.on('error', () => resolve(false));
  });
}

/** TCP 연결만 되면 산 것으로 본다. Postgres처럼 HTTP를 안 말하는 포트용. */
function probeTcp(port, timeout = 3000) {
  return new Promise((resolve) => {
    const sock = net.connect({ host: '127.0.0.1', port });
    const finish = (ok) => {
      sock.removeAllListeners();
      sock.on('error', () => {});
      sock.destroy();
      resolve(ok);
    };
    sock.setTimeout(timeout);
    sock.once('connect', () => finish(true));
    sock.once('timeout', () => finish(false));
    sock.once('error', () => finish(false));
  });
}

function probe(s, timeout = 3000) {
  return s.probe === 'tcp' ? probeTcp(s.port, timeout) : probeHttp(s.port, timeout);
}

function findPidsOnPort(port) {
  return new Promise((resolve) => {
    execFile('netstat', ['-ano'], { windowsHide: true, maxBuffer: 16 * 1024 * 1024 }, (err, out) => {
      if (err || !out) return resolve([]);
      const pids = new Set();
      for (const line of out.split(/\r?\n/)) {
        const cols = line.trim().split(/\s+/);
        if (cols.length >= 5 && cols[0].startsWith('TCP') && /LISTENING/i.test(cols[3]) && cols[1].endsWith(':' + port)) {
          const pid = Number(cols[4]);
          if (pid > 4 && pid !== process.pid) pids.add(pid);
        }
      }
      resolve([...pids]);
    });
  });
}

function killTree(pid) {
  return new Promise((resolve) => {
    execFile('taskkill', ['/PID', String(pid), '/T', '/F'], { windowsHide: true }, () => resolve());
  });
}

/** Windows에서 이전 실행이 `>> 로그`로 파일을 붙잡고 있으면 rename/append가 EBUSY가 난다. */
function pickRunLog(preferred) {
  try {
    rotate(preferred);
    fs.appendFileSync(preferred, '');
    return preferred;
  } catch {
    const alt = preferred.replace(/\.log$/i, `-${Date.now()}.log`);
    wlog(`로그 파일이 잠겨 있어 ${path.basename(alt)}에 씁니다`);
    return alt;
  }
}

function spawnDetached(command, cwd, logFile, id) {
  const out = pickRunLog(logFile);
  try {
    fs.appendFileSync(out, `\n===== [${ts()}] 실행: ${command} (cwd: ${cwd}) =====\n`);
  } catch { /* 헤더를 못 써도 기동은 한다 */ }
  const launcher = path.join(LOG_DIR, `${id}.launch.cmd`);
  fs.writeFileSync(launcher, [
    '@echo off',
    '@chcp 65001 >nul',
    `cd /d "${cwd}"`,
    `${command} >> "${out}" 2>&1`,
    '',
  ].join('\r\n'));
  const child = spawn('wscript.exe', ['//B', '//Nologo', RUN_HIDDEN_VBS, launcher], {
    detached: true, windowsHide: true, stdio: 'ignore',
  });
  child.unref();
}

function runToCompletion(command, cwd, logFile, label) {
  return new Promise((resolve, reject) => {
    rotate(logFile);
    const fd = fs.openSync(logFile, 'a');
    fs.writeSync(fd, `\n===== [${ts()}] ${label}: ${command} (cwd: ${cwd}) =====\n`);
    const child = spawn(command, { cwd, shell: true, windowsHide: true, stdio: ['ignore', fd, fd] });
    child.on('close', (code) => {
      fs.closeSync(fd);
      if (code === 0) resolve();
      else reject(new Error(`${label} 실패 (종료코드 ${code}) — ${label} 로그를 확인하세요`));
    });
    child.on('error', (e) => { fs.closeSync(fd); reject(e); });
  });
}

// ---------- 시작 / 중지 ----------
async function ensureDependsOn(s) {
  if (!s.dependsOn) return;
  if (s.dependsOn === s.id) throw new Error('선행 서버가 자기 자신입니다.');
  const dep = cfg.servers.find((x) => x.id === s.dependsOn);
  if (!dep) throw new Error(`선행 서버(${s.dependsOn})가 설정에 없습니다.`);
  if (await probe(dep, 1500)) return;
  wlog(`[${s.name}] 선행 ${dep.name}을 먼저 켭니다`);
  await doStart(dep, getRt(dep.id));
  if (!(await probe(dep, 1500))) throw new Error(`${dep.name}이 켜지지 않아 시작할 수 없습니다.`);
}

async function doStart(s, rt, { skipBuildCheck = false } = {}) {
  if (await probe(s)) { // 이미 떠 있으면 그대로 인수
    rt.phase = 'running'; rt.fails = 0;
    rt.startedAt = rt.startedAt || Date.now();
    return;
  }
  await ensureDependsOn(s);

  const cwd = resolveCwd(s);
  if (!fs.existsSync(cwd)) throw new Error(`실행 폴더가 없습니다: ${cwd}`);

  // 빌드 산출물이 없으면 먼저 빌드 (예: Next.js 첫 실행)
  if (!skipBuildCheck && s.buildCommand && s.builtMarker && !fs.existsSync(path.resolve(cwd, s.builtMarker))) {
    rt.phase = 'building';
    wlog(`[${s.name}] 빌드 산출물이 없어 빌드를 시작합니다: ${s.buildCommand}`);
    await runToCompletion(s.buildCommand, cwd, logPath(s.id, 'build'), '빌드');
  }

  const pids = await findPidsOnPort(s.port);
  if (pids.length) {
    throw new Error(`포트 ${s.port}가 응답 없는 프로세스(PID ${pids.join(', ')})에 점유되어 있습니다. [끄기]로 정리한 뒤 다시 켜세요.`);
  }

  rt.phase = 'starting';
  spawnDetached(s.command, cwd, logPath(s.id, 'run'), s.id);
  wlog(`[${s.name}] 시작: ${s.command}`);

  const ok = await waitFor(() => probe(s), START_TIMEOUT_MS, 1500);
  if (ok) {
    rt.phase = 'running'; rt.startedAt = Date.now();
    rt.lastError = null; rt.fails = 0;
    wlog(`[${s.name}] 정상 응답 확인 (포트 ${s.port})`);
  } else {
    throw new Error(`${START_TIMEOUT_MS / 1000}초 내 응답이 없습니다 — 실행 로그를 확인하세요.`);
  }
}

async function doStop(s, rt, { manual = true } = {}) {
  rt.phase = 'stopping';
  const pids = await findPidsOnPort(s.port);
  for (const pid of pids) await killTree(pid);
  const freed = await waitFor(
    async () => !(await probe(s, 1500)) && (await findPidsOnPort(s.port)).length === 0,
    STOP_TIMEOUT_MS, 700,
  );
  if (!freed) throw new Error(`종료를 확인하지 못했습니다 — 포트 ${s.port}가 여전히 점유 중입니다.`);
  rt.phase = 'stopped'; rt.startedAt = null; rt.fails = 0;
  wlog(`[${s.name}] 종료됨${manual ? ' (수동)' : ''}`);
}

// 한 서버에 하나의 작업만 허용. 작업 실패 시 failed 로 남긴다.
async function runOp(s, name, fn) {
  const rt = getRt(s.id);
  if (rt.opInFlight) {
    const e = new Error(`이미 '${rt.opInFlight}' 작업이 진행 중입니다.`);
    e.status = 409;
    throw e;
  }
  rt.opInFlight = name;
  try {
    await fn(rt);
  } catch (e) {
    rt.lastError = String(e.message || e);
    rt.phase = 'failed';
    rt.startedAt = null;
    wlog(`[${s.name}] ${name} 실패: ${rt.lastError}`);
  } finally {
    rt.opInFlight = null;
  }
}

const actions = {
  start: (s) => runOp(s, 'start', (rt) => doStart(s, rt)),
  stop: (s) => runOp(s, 'stop', (rt) => doStop(s, rt, { manual: true })),
  restart: (s) => runOp(s, 'restart', async (rt) => {
    if (await probe(s, 1500)) await doStop(s, rt, { manual: false });
    await doStart(s, rt);
  }),
  update: (s) => runOp(s, 'update', async (rt) => {
    if (!s.buildCommand) throw new Error('빌드 명령이 설정되지 않은 서버입니다.');
    rt.phase = 'building';
    await runToCompletion(s.buildCommand, resolveCwd(s), logPath(s.id, 'build'), '빌드');
    if (await probe(s, 1500)) await doStop(s, rt, { manual: false });
    await doStart(s, rt, { skipBuildCheck: true });
  }),
};

// ---------- 감시 루프 ----------
async function monitorTick() {
  for (const s of cfg.servers) {
    const rt = getRt(s.id);
    if (rt.opInFlight) continue;
    const up = await probe(s, 2500);
    if (up) {
      if (rt.phase !== 'running') {
        rt.phase = 'running';
        rt.startedAt = rt.startedAt || Date.now();
        rt.lastError = null;
      }
      rt.fails = 0;
      continue;
    }
    rt.fails++;
    if (rt.fails < 2) continue; // 일시적 무응답 1회는 무시
    // 자동 복구는 하지 않는다 — 상태만 갱신하고 사용자가 수동으로 켜도록 둔다
    if (rt.phase === 'running') {
      rt.phase = 'crashed'; rt.startedAt = null;
      wlog(`[${s.name}] 응답 중단 감지 (포트 ${s.port}) — 수동으로 켜세요`);
    } else if (rt.phase === 'unknown') {
      rt.phase = 'stopped';
    }
  }
}

async function initialSweep() {
  for (const s of cfg.servers) {
    const rt = getRt(s.id);
    if (await probe(s)) {
      rt.phase = 'running'; rt.startedAt = Date.now();
      wlog(`[${s.name}] 이미 실행 중 (포트 ${s.port}) — 감시만 시작합니다`);
    } else {
      rt.phase = 'stopped';
      wlog(`[${s.name}] 꺼짐 — 자동 시작하지 않습니다 (수동으로 켜세요)`);
    }
  }
}

// ---------- 설정 검증 / 저장 ----------
function validateServers(list, prevById) {
  if (!Array.isArray(list) || list.length > 20) throw new Error('서버 목록 형식이 올바르지 않습니다.');
  const busyPhases = new Set(['running', 'starting', 'building', 'stopping']);
  const ports = new Set();
  const taken = new Set();
  const out = [];

  for (const raw of list) {
    const name = String(raw.name || '').trim();
    const command = String(raw.command || '').trim();
    const cwd = String(raw.cwd || '').trim();
    const port = Number(raw.port);
    if (!name || name.length > 40) throw new Error('서버 이름은 1~40자여야 합니다.');
    if (!command || command.length > 300) throw new Error(`[${name}] 실행 명령어를 입력하세요.`);
    if (!cwd) throw new Error(`[${name}] 실행 폴더를 입력하세요.`);
    if (!Number.isInteger(port) || port < 1 || port > 65535) throw new Error(`[${name}] 포트가 올바르지 않습니다.`);
    if (ports.has(port)) throw new Error(`포트 ${port}가 중복 등록되어 있습니다.`);
    ports.add(port);
    const resolved = path.resolve(HERE, cwd);
    if (!fs.existsSync(resolved)) throw new Error(`[${name}] 실행 폴더가 존재하지 않습니다: ${resolved}`);

    let id = String(raw.id || '').trim();
    const prev = id ? prevById.get(id) : null;
    if (prev) {
      const rt = getRt(id);
      const changed = prev.command !== command || Number(prev.port) !== port || resolveCwd(prev) !== resolved;
      if (changed && busyPhases.has(rt.phase)) {
        throw new Error(`[${name}] 실행 중에는 명령어/포트/폴더를 변경할 수 없습니다. 먼저 서버를 끄세요.`);
      }
    } else {
      id = slugify(name, taken);
    }
    taken.add(id);

    const probeKind = String(raw.probe || 'http').trim();
    if (probeKind !== 'http' && probeKind !== 'tcp') throw new Error(`[${name}] 확인 방식은 http 또는 tcp여야 합니다.`);
    const dependsOn = String(raw.dependsOn || '').trim() || undefined;

    out.push({
      id, name, cwd, command, port,
      probe: probeKind,
      dependsOn,
      buildCommand: String(raw.buildCommand || '').trim() || undefined,
      builtMarker: String(raw.builtMarker || '').trim() || undefined,
    });
  }

  // 실행 중인 서버는 목록에서 제거 불가
  for (const [id, prev] of prevById) {
    if (!out.some((s) => s.id === id) && busyPhases.has(getRt(id).phase)) {
      throw new Error(`[${prev.name}] 켜져 있는 서버는 삭제할 수 없습니다. 먼저 끄세요.`);
    }
  }
  return out;
}

// ---------- HTTP 관제 API ----------
function json(res, code, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' });
  res.end(body);
}

function readBody(req, limit = 200 * 1024) {
  return new Promise((resolve, reject) => {
    let size = 0;
    const chunks = [];
    req.on('data', (c) => {
      size += c.length;
      if (size > limit) { reject(new Error('요청이 너무 큽니다.')); req.destroy(); return; }
      chunks.push(c);
    });
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    req.on('error', reject);
  });
}

function statusPayload() {
  return {
    watchdog: { version: VERSION, pid: process.pid, startedAt: bootAt, controlPort: cfg.controlPort, configPath: CONFIG_PATH },
    servers: cfg.servers.map((s) => {
      const rt = getRt(s.id);
      return {
        id: s.id, name: s.name, port: s.port, command: s.command,
        cwd: resolveCwd(s), hasBuild: !!s.buildCommand,
        phase: rt.phase, opInFlight: rt.opInFlight, lastError: rt.lastError,
        uptimeSec: rt.startedAt ? Math.floor((Date.now() - rt.startedAt) / 1000) : null,
      };
    }),
  };
}

async function handleRequest(req, res) {
  const host = String(req.headers.host || '');
  if (!/^(localhost|127\.0\.0\.1):\d+$/i.test(host)) return json(res, 403, { error: '로컬 접근만 허용됩니다.' });

  const u = new URL(req.url, 'http://localhost');
  const parts = u.pathname.split('/').filter(Boolean).map(decodeURIComponent);

  if (req.method === 'GET') {
    if (parts.length === 0) {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
      return res.end(fs.readFileSync(CONTROL_HTML));
    }
    if (u.pathname === '/favicon.ico') { res.writeHead(204); return res.end(); }
    if (u.pathname === '/api/status') return json(res, 200, statusPayload());
    if (u.pathname === '/api/config') return json(res, 200, { controlPort: cfg.controlPort, servers: cfg.servers });
    if (u.pathname === '/api/watchdog/logs') return json(res, 200, { text: tailFile(WATCHDOG_LOG) });
    if (parts[0] === 'api' && parts[1] === 'servers' && parts[3] === 'logs') {
      const s = cfg.servers.find((x) => x.id === parts[2]);
      if (!s) return json(res, 404, { error: '해당 서버가 없습니다.' });
      const type = u.searchParams.get('type') === 'build' ? 'build' : 'run';
      return json(res, 200, { text: tailFile(logPath(s.id, type)) });
    }
    return json(res, 404, { error: 'not found' });
  }

  if (req.method === 'POST') {
    // 브라우저 교차출처 요청 차단: 커스텀 헤더 강제 (preflight 유발)
    if (req.headers['x-control'] !== '1') return json(res, 403, { error: '잘못된 요청입니다.' });

    // 스테이션 자기 종료 (서버는 유지) — 설정 패널의 [서버관리 종료]
    // (완전 제거/등록 해제는 UI에 두지 않는다 — 관리자용: node setup.mjs --remove)
    if (u.pathname === '/api/watchdog/stop') {
      wlog('서버관리 종료 요청(설정 패널) — 서버는 계속 돌아갑니다');
      try { fs.unlinkSync(PID_FILE); } catch { /* ignore */ }
      json(res, 200, { ok: true });
      setTimeout(() => process.exit(0), 300);
      return;
    }

    if (parts[0] === 'api' && parts[1] === 'servers' && parts[3] && actions[parts[3]]) {
      const s = cfg.servers.find((x) => x.id === parts[2]);
      if (!s) return json(res, 404, { error: '해당 서버가 없습니다.' });
      const rt = getRt(s.id);
      if (rt.opInFlight) return json(res, 409, { error: `이미 '${rt.opInFlight}' 작업이 진행 중입니다.` });
      actions[parts[3]](s); // 백그라운드 수행 — UI는 상태 폴링으로 진행 확인
      return json(res, 202, { ok: true });
    }

    if (u.pathname === '/api/config') {
      try {
        const body = JSON.parse(await readBody(req));
        const prevById = new Map(cfg.servers.map((s) => [s.id, s]));
        const servers = validateServers(body.servers, prevById);
        const controlPort = Number(body.controlPort);
        const portChanged = Number.isInteger(controlPort) && controlPort !== cfg.controlPort;
        cfg.servers = servers;
        if (portChanged && controlPort >= 1024 && controlPort <= 65535) cfg.controlPort = controlPort;
        for (const id of [...runtimes.keys()]) {
          if (!servers.some((s) => s.id === id)) runtimes.delete(id);
        }
        saveConfig();
        wlog(`설정 저장됨 (서버 ${servers.length}개)`);
        return json(res, 200, { ok: true, notice: portChanged ? '관제 포트 변경은 스테이션 재시작 후 적용됩니다.' : null });
      } catch (e) {
        return json(res, 400, { error: String(e.message || e) });
      }
    }
    return json(res, 404, { error: 'not found' });
  }

  json(res, 405, { error: 'method not allowed' });
}

// ---------- 기동 ----------
let bootAt = Date.now();

process.on('uncaughtException', (e) => wlog(`uncaughtException: ${e.stack || e}`));
process.on('unhandledRejection', (e) => wlog(`unhandledRejection: ${e}`));

fs.mkdirSync(LOG_DIR, { recursive: true });
loadConfig();

const server = http.createServer((req, res) => {
  handleRequest(req, res).catch((e) => {
    wlog(`요청 처리 오류: ${e.stack || e}`);
    try { json(res, 500, { error: '내부 오류' }); } catch { /* ignore */ }
  });
});

server.on('error', (e) => {
  if (e.code === 'EADDRINUSE') {
    console.log(`스테이션이 이미 열려 있습니다 (포트 ${cfg.controlPort}). 이 창은 닫아도 됩니다.`);
    process.exit(0);
  }
  wlog(`관제 서버 오류: ${e}`);
  process.exit(1);
});

server.listen(cfg.controlPort, '127.0.0.1', () => {
  try { fs.writeFileSync(PID_FILE, String(process.pid)); } catch { /* ignore */ }
  wlog(`스테이션 v${VERSION} 열림 — http://localhost:${cfg.controlPort} (서버 ${cfg.servers.length}개 등록됨)`);
  initialSweep();
  setInterval(() => { monitorTick().catch((e) => wlog(`감시 루프 오류: ${e}`)); }, MONITOR_INTERVAL_MS);
});
