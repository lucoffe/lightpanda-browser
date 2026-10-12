// Reproducer: Lightpanda's idle memory climbs to the largest page served.
// Serves synthetic pages whose memory peak rises over the run, drives
// `lightpanda serve` with one CDP connection per render, and samples the
// process from /proc while it is idle between renders. Linux only.
import { spawn, execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { createServer } from 'node:http';
import { connect as tcp } from 'node:net';
import { parseArgs } from 'node:util';
import puppeteer from 'puppeteer-core';

const { values: o } = parseArgs({ options: {
  bin: { type: 'string', default: './lightpanda' }, // lightpanda binary
  port: { type: 'string', default: '9222' }, // CDP port
  renders: { type: 'string', default: '300' }, // total renders (one connection each)
  clients: { type: 'string', default: '1' }, // concurrent CDP connections
  js: { type: 'string', default: '500' }, // KiB of generated JS per page...
  ramp: { type: 'string', default: '50' }, // ...growing by that much every N renders
  every: { type: 'string' }, // idle sample every N renders (default: clients)
  idle: { type: 'string', default: '500' }, // ms to wait, idle, before a sample
  'trim-at-end': { type: 'boolean', default: false }, // gdb: what malloc_trim(0) would free
} });
const N = +o.renders, C = +o.clients, JS = +o.js, RAMP = +o.ramp, EVERY = +(o.every ?? C);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const MiB = (kb) => kb / 1024;

// Pages: a JS bundle of distinct functions (parse, compile, hot loops for the
// optimizing tiers), sized by the render index, plus some DOM building.
function bundle(i) {
  const kb = JS * (1 + Math.floor(i / RAMP)), out = ['(function(){var acc=0;'];
  for (let n = 0, len = 0; len < kb * 1024; n++) {
    const f = `f${i}_${n}`, c = `c${i}_${n}`;
    const s = `function ${f}(a,b){var o={k${n}:a,v:b,s:'${n}-'+a};for(var j=0;j<(a&7);j++){o.v=(o.v*31+j)|0;if(o.s.length>${n % 50 + 10})o.s=o.s.slice(1);}return o.v^o.s.length;}\n` +
      `var ${c}=class{constructor(x){this.x=x;this.y=[x,x+1,'q${n}'];}m(){return this.y.length+this.x;}};\n` +
      (n % 20 === 0 ? `for(var h=0;h<3000;h++){acc=(acc+${f}(h,acc))|0;acc+=new ${c}(h).m();}\n` : `acc=(acc+${f}(${n},acc))|0;\n`);
    out.push(s); len += s.length;
  }
  out.push('window.__acc=acc;})();');
  return out.join('');
}
const page = (i) => `<!doctype html><html><head><title>render ${i}</title></head><body><div id=root></div>
<script>const r=document.getElementById('root');for(let k=0;k<3000;k++){const d=document.createElement('div');d.className='row r'+(k%7);d.textContent='row '+k+' of render ${i}';r.appendChild(d);}
r.innerHTML+=Array.from({length:500},(_, k)=>'<p data-k="'+k+'"><span>'+k+'</span></p>').join('');for(const p of [...r.querySelectorAll('p')].slice(250))p.remove();</script>
<script src="/b/${i}.js"></script></body></html>`;
const http = createServer((req, res) => {
  const m = req.url.match(/^\/(p|b)\/(\d+)/);
  if (!m) return res.writeHead(404).end();
  res.writeHead(200, { 'content-type': m[1] === 'p' ? 'text/html' : 'text/javascript' });
  res.end(m[1] === 'p' ? page(+m[2]) : bundle(+m[2]));
});
await new Promise((r) => http.listen(0, '127.0.0.1', r));
const origin = `http://127.0.0.1:${http.address().port}`;

// Lightpanda, and what /proc says about it.
const lp = spawn(o.bin, ['serve', '--host', '127.0.0.1', '--port', o.port, '--cdp-max-connections', '64', '--log-level', 'err'], { stdio: ['ignore', 'inherit', 'inherit'] });
lp.on('exit', (code, sig) => { if (!stopping) { console.error(`lightpanda exited (${code ?? sig})`); process.exit(1); } });
let stopping = false;
for (let ok = false; !ok; await sleep(100)) ok = await new Promise((r) => tcp(+o.port, '127.0.0.1').on('connect', function () { this.end(); r(true); }).on('error', () => r(false)));
function proc() {
  const s = readFileSync(`/proc/${lp.pid}/smaps_rollup`, 'utf8'), kb = (k) => +(s.match(new RegExp(`^${k}:\\s+(\\d+)`, 'm'))?.[1] ?? 0);
  const st = readFileSync(`/proc/${lp.pid}/stat`, 'utf8'), f = st.slice(st.lastIndexOf(')') + 2).split(' ');
  // stat fields 10 (minflt), 14+15 (utime+stime, in 1/100 s), counted from field 3
  return { pss: MiB(kb('Pss')), anon: MiB(kb('Pss_Anon')), cpuMs: (+f[11] + +f[12]) * 10, minflt: +f[7] };
}
let peak = 0;
const watch = setInterval(() => { peak = Math.max(peak, proc().pss); }, 200);

// Clients: one connection per render. Every EVERY renders, all clients pause,
// the server is left idle for a moment, and the process is sampled.
let next = 0, done = 0, failed = 0, active = 0, nextAt = EVERY, gate = null, pausedMs = 0;
const samples = [];
async function idleSample() {
  const t0 = Date.now(); let release; gate = new Promise((r) => (release = r));
  while (active > 0) await sleep(5);
  await sleep(+o.idle);
  samples.push({ renders: done, ...proc() });
  gate = null; release(); pausedMs += Date.now() - t0;
}
async function render(i) {
  const browser = await puppeteer.connect({ browserWSEndpoint: `ws://127.0.0.1:${o.port}`, protocolTimeout: 120000 });
  try {
    const ctx = await browser.createBrowserContext();
    const p = await ctx.newPage();
    await p.goto(`${origin}/p/${i}`, { waitUntil: 'load', timeout: 120000 });
  } finally { await browser.disconnect(); }
}
async function client() {
  for (;;) {
    while (gate) await gate;
    const i = next++;
    if (i >= N) return;
    active++;
    try { await render(i); } catch (e) { failed++; console.error(`render ${i}: ${e.message}`); }
    active--; done++;
    if (done >= nextAt && !gate) { nextAt += EVERY; await idleSample(); }
  }
}
await sleep(+o.idle);
const start = proc(), t0 = Date.now();
await Promise.all(Array.from({ length: C }, client));
const busyS = (Date.now() - t0 - pausedMs) / 1000, end = samples.at(-1);
clearInterval(watch);

// Report: idle floors per fifth of the run (median of the idle samples).
const med = (a) => { const s = [...a].sort((x, y) => x - y); return s.length ? s[s.length >> 1] : NaN; };
const fifths = [0, 1, 2, 3, 4].map((k) => samples.filter((s) => Math.min(4, Math.floor(((s.renders - 1) * 5) / N)) === k));
const fl = fifths.map((g) => ({ pss: med(g.map((s) => s.pss)), anon: med(g.map((s) => s.anon)) }));
const r = {
  renders: done, failed, clients: C, busyS, rendersPerS: done / busyS,
  cpuMsPerRender: (end.cpuMs - start.cpuMs) / done, minfltPerRender: (end.minflt - start.minflt) / done,
  peakPss: peak, startPss: start.pss, startAnon: start.anon, floors: fl,
  growthPss: fl[4].pss / fl[0].pss, growthAnon: fl[4].anon / fl[0].anon,
};
const f1 = (x) => x.toFixed(1);
console.log(`${done} renders (${failed} failed), ${C} client(s): ${r.rendersPerS.toFixed(2)} renders/s, ${f1(r.cpuMsPerRender)} ms CPU/render, ${Math.round(r.minfltPerRender)} minor faults/render, peak PSS ${f1(peak)} MiB`);
console.log(`idle before the first render: PSS ${f1(start.pss)} MiB, anon ${f1(start.anon)} MiB`);
console.log('idle floor (median) by fifth of the run, MiB:\n  fifth   PSS     anon');
fl.forEach((x, k) => console.log(`  ${k + 1}    ${f1(x.pss).padStart(6)}  ${f1(x.anon).padStart(6)}`));
console.log(`growth, last fifth / first: PSS ${r.growthPss.toFixed(2)}x, anon ${r.growthAnon.toFixed(2)}x`);
if (o['trim-at-end']) { // needs ptrace rights (sudo); gdb drops file pages, so compare anon only
  const before = proc().anon;
  execFileSync('gdb', ['-p', String(lp.pid), '-batch', '-ex', 'call (int)malloc_trim(0)'], { stdio: 'ignore' });
  r.trim = { anonBefore: before, anonAfter: proc().anon };
  console.log(`malloc_trim(0) at idle: anon ${f1(before)} -> ${f1(r.trim.anonAfter)} MiB`);
}
console.log('RESULT ' + JSON.stringify(r));
stopping = true; lp.kill('SIGTERM'); http.close();
